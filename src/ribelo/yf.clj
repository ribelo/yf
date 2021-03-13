(ns ribelo.yf
  (:refer-clojure :exclude [float])
  (:require
   [clojure.string :as str]
   [taoensso.encore :as e]
   [hato.client :as http]
   [hickory.core :as hc]
   [hickory.select :as hs]
   [com.wsscode.pathom3.cache :as p.cache]
   [com.wsscode.pathom3.connect.operation :as pco]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.interface.eql :as p.eql])
  (:import
   (java.net CookieHandler CookieManager)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn change-ns-in-map [ns map]
  (persistent!
   (reduce-kv
    (fn [acc k v]
      (assoc! acc (e/merge-keywords [ns (name k)]) v))
    (transient {})
    map)))

(deftype Cache [^java.util.concurrent.ConcurrentHashMap x
                ^java.util.concurrent.ConcurrentHashMap ttl-map
                ^long ttl]
  clojure.lang.IAtom
  (reset [_ _]
    (.clear x)
    (.clear ttl-map))
  p.cache/CacheStore
  (p.cache/-cache-lookup-or-miss [_ k f]
    (doseq [k* (.keySet x)]
      (let [t (- ^long (e/now-udt) ^long (.getOrDefault ttl-map k* 0))]
        (when (> t ttl)
          (.remove x k*)
          (.remove ttl-map k*))))
    (if-let [v (.get x k)]
      v
      (let [res (f)]
        (.put ttl-map k (e/now-udt))
        (.put x k res)
        res))))

(def ttl-cache_ (->Cache (java.util.concurrent.ConcurrentHashMap.)
                         (java.util.concurrent.ConcurrentHashMap.)
                         (e/ms :mins 15)))

(defmacro try-times
  {:style/indent 1}
  [{:keys [max-retries sleep]} & body]
  `(loop [i# 0]
     (if-let [result# (try (or (do ~@body) true)
                           (catch Exception e#
                             (when (>= i# ~max-retries)
                               (throw e#))))]
       result#
       (do
         (when (e/pos-num? ~sleep)
           (Thread/sleep ~sleep))
         (recur (inc i#))))))

(defn- str-time->epoch ^long [s]
  (quot (.getTime (.parse (e/simple-date-format "yyyy-MM-dd" :timezone :utc) s)) 1000))

(def ^:private cookie-handler (do (CookieHandler/setDefault (CookieManager.)) (CookieHandler/getDefault)))

(def ^:private memoized-http-get
  (e/memoize
    (fn
      ([url opts] (http/get url opts))
      ([url] (memoized-http-get url {})))))

(defn- get-crumb [ticker]
  (try-times {:max-retries 5 :sleep 3000}
    (let [url     "https://finance.yahoo.com/quote/%s/history"
          resp    (http/get (format url (str/upper-case (name ticker)))
                            {:cookie-handler cookie-handler})
          body    (:body resp)
          crumb   (second (re-find #"\"CrumbStore\":\{\"crumb\":\"(.{11})\"\}" body))]
      (if crumb
        crumb
        (throw (ex-info "empty response" {:symbol ticker}))))))

(defn- get-data
  ([ticker] (get-data ticker {}))
  ([ticker {:keys [start-time end-time interval]
            :or   {start-time "2000-01-01"
                   end-time   (.format (e/simple-date-format "yyyy-MM-dd") (java.util.Date.))
                   interval   "1d"}}]
   (e/if-let [crumb       (get-crumb ticker)
              url         "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s"
              start-time* (str-time->epoch start-time)
              end-time*   (str-time->epoch end-time)
              event       "history"]
     (->> (http/get (format url
                            (str/upper-case (name ticker))
                            start-time*
                            end-time*
                            interval
                            event
                            crumb)
                    {:cookie-handler cookie-handler})
          :body
          clojure.string/split-lines
          (into []
                (comp
                 (drop 1)
                 (map #(clojure.string/split % #","))
                 (filter (fn [[date]] (>= (str-time->epoch date) start-time*)))
                 (map (fn [[date open high low close _ volume]]
                        {:date   date
                         :open   (e/as-?float open)
                         :high   (e/as-?float high)
                         :low    (e/as-?float low)
                         :close  (e/as-?float close)
                         :volume (e/as-?int volume)}))
                 (map (partial change-ns-in-map :yf.quotes))
                 (filter (fn [m] (every? identity (vals m)))))))
     (throw (ex-info "empty response" {:symbol ticker})))))

(pco/defresolver quotes
  [{:yf/keys [ticker start-time end-time]}]
  {::pco/input       [:yf/ticker (pco/? :yf/start-time) (pco/? :yf/end-time)]
   ::pco/output      [{:yf/quotes [:yf.quotes/date :yf.quotes/open :yf.quotes/high
                                   :yf.quotes/low :yf.quotes/close :yf.quotes/volume]}]
   ::pco/cache-store ::ttl-cache}
  {:yf/quotes
   (get-data ticker (e/assoc-some {}
                                  :start-time start-time
                                  :end-time   end-time))})
(comment
  (p.eql/process env {:yf/ticker :msft :yf/start-time "2020-01-02"} [:yf/quotes]))

(pco/defresolver quote-page-htree
  [{:yf/keys [ticker]}]
  {::pco/cache-store ::ttl-cache}
  {:yf.quote.page/htree
   (->> (http/get (str "https://finance.yahoo.com/quote/" (str/upper-case (name ticker)))
                  {:cookie-handler cookie-handler})
        :body
        hc/parse
        hc/as-hickory)})

(def stock?
  (e/memoize
    (fn [ticker]
      (let [ticker (cond (map? ticker) (ticker :yf/ticker) (string? ticker) ticker)
            {:keys [yf.quote.page/htree]} (quote-page-htree {:yf/ticker ticker})]
        (->> htree
             (hs/select (hs/child
                         (hs/attr "data-test" #(= % "MARKET_CAP-value"))
                         (hs/tag :span)))
             not-empty
             nil?
             not)))))

(comment
  (stock? "msft"))

(defn etf? [ticker]
  (not (stock? ticker)))

(comment
  (etf? "msft"))

(pco/defresolver key-stats-htree
  [{:yf/keys [ticker]}]
  {::pco/cache-store ::ttl-cache}
  {:yf.key-stats.page/htree
   (->> (http/get (format "https://finance.yahoo.com/quote/%s/key-statistics" (str/upper-case (name ticker)))
                  {:cookie-handler cookie-handler})
        :body
        hc/parse
        hc/as-hickory)})

(def ^:private unit->value
  {"m" 1e6
   "b" 1e9
   "t" 1e12
   "%" 0.01})

(defn- ->value [s]
  (when (string? s)
    (let [num  (->> (clojure.string/replace s #"," "") (re-find #"([0-9]*.[0-9]+|[0-9]+)") first e/as-?float)
          unit (str (last s))]
      (when num
        (* ^double num ^double (unit->value (str/lower-case unit) 1.0))))))

(defn- find-value-by-description [re htree]
  (->> htree
       (hs/select (hs/follow
                   (hs/has-child (hs/find-in-text re))
                   (hs/tag :td)))
       first
       :content
       first))

(pco/defresolver market-cap
  [{:yf.key-stats.page/keys [htree]}]
  {:yf/market-cap
   (->> htree
        (find-value-by-description #"(?i)^market cap")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/market-cap])
  ;; => #:yf{:market-cap 1.72E12}
  )

(pco/defresolver enterprise-value
  [{:yf.key-stats.page/keys [htree]}]
  {:yf/enterprise-value
   (->> htree
        (find-value-by-description #"(?i)^enterprise value")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/enterprise-value])
  ;; => #:yf{:enterprise-value 1.7E12}
  )

(pco/defresolver trailing-pe-ratio
  [{:yf.key-stats.page/keys [htree]}]
  {:yf/trailing-pe-ratio
   (->> htree
        (find-value-by-description #"(?i)^trailing p/e")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/trailing-pe-ratio])
  ;; => #:yf{:trailing-pe-ratio 33.9}
  )

(pco/defresolver forward-pe-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/forward-pe-ratio
   (->> htree
        (find-value-by-description #"(?i)^forward p/e")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/forward-pe-ratio])
  ;; => #:yf{:forward-pe-ratio 28.11}
  )

(pco/defresolver peg-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/peg-ratio
   (->> htree
        (find-value-by-description #"(?i)^peg ratio")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/peg-ratio])
  ;; => #:yf{:peg-ratio 1.82}
  )

(pco/defresolver price-to-sales-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/price-to-sales-ratio
   (->> htree
        (find-value-by-description #"(?i)^price/sales")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/price-to-sales-ratio])
  ;; => #:yf{:price-to-sales-ratio 11.19}
  )

(pco/defresolver price-to-book-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/price-to-book-ratio
   (->> htree
        (find-value-by-description #"(?i)^enterprise value")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/price-to-book-ratio])
  ;; => #:yf{:price-to-book-ratio 1.7E12}
  )

(pco/defresolver enterprise-value-to-revenue-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/enterprise-value-to-revenue-ratio
   (->> htree
        (find-value-by-description #"(?i)^enterprise value/revenue")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/enterprise-value-to-revenue-ratio])
  ;; => #:yf{:enterprise-value-to-revenue-ratio 11.07}
  )

(pco/defresolver enterprise-value-to-ebitda-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/enterprise-value-to-ebitda-ratio
   (->> htree
        (find-value-by-description #"(?i)^enterprise value/ebitda")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/enterprise-value-to-ebitda-ratio])
  ;; => #:yf{:enterprise-value-to-ebitda-ratio 23.68}
  )

(pco/defresolver fiscal-year-ends [{:yf.key-stats.page/keys [htree]}]
  {:yf/fiscal-year-ends
   (when-let [s (->> htree
                     (find-value-by-description #"(?i)^fiscal year ends"))]
     (when (string? s)
       (.format
        (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
        (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s))))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/fiscal-year-ends])
  ;; => #:yf{:fiscal-year-ends "2020-06-29"}
  )

(pco/defresolver most-recent-quarter [{:yf.key-stats.page/keys [htree]}]
  {:yf/most-recent-quarter
   (when-let [s (->> htree
                     (find-value-by-description #"(?i)^most recent quarter"))]
     (when (string? s)
       (.format
        (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
        (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s))))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/most-recent-quarter])
  ;; => #:yf{:most-recent-quarter "2020-12-30"}
  )

(pco/defresolver profit-margin [{:yf.key-stats.page/keys [htree]}]
  {:yf/profit-margin
   (->> htree
        (find-value-by-description #"(?i)^profit margin")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/profit-margin])
  ;; => #:yf{:profit-margin 0.3347}
  )

(pco/defresolver operating-margin [{:yf.key-stats.page/keys [htree]}]
  {:yf/operating-margin
   (->> htree
        (find-value-by-description #"(?i)^operating margin")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/operating-margin])
  ;; => #:yf{:operating-margin 0.3924}
  )

(pco/defresolver roa [{:yf.key-stats.page/keys [htree]}]
  {:yf/roa
   (->> htree
        (find-value-by-description #"(?i)^return on assets")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/roa])
  ;; => #:yf{:roa 0.12810000000000002}
  )

(pco/defresolver roe [{:yf.key-stats.page/keys [htree]}]
  {:yf/roe
   (->> htree
        (find-value-by-description #"(?i)^return on equity")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/roe])
  ;; => #:yf{:roe 0.42700000000000005}
  )

(pco/defresolver revenue [{:yf.key-stats.page/keys [htree]}]
  {:yf/revenue
   (->> htree
        (find-value-by-description #"(?i)^revenue$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/revenue])
  ;; => #:yf{:revenue 1.5328E11}
  )

(pco/defresolver revenue-per-share [{:yf.key-stats.page/keys [htree]}]
  {:yf/revenue-per-share
   (->> htree
        (find-value-by-description #"(?i)^revenue per share")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/revenue-per-share])
  ;; => #:yf{:revenue-per-share 20.23}
  )

(pco/defresolver quarterly-revenue-growth [{:yf.key-stats.page/keys [htree]}]
  {:yf/quarterly-revenue-growth
   (->> htree
        (find-value-by-description #"(?i)^quarterly revenue growth$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/quarterly-revenue-growth])
  ;; => #:yf{:quarterly-revenue-growth 0.167}
  )

(pco/defresolver gross-profit [{:yf.key-stats.page/keys [htree]}]
  {:yf/gross-profit
   (->> htree
        (find-value-by-description #"(?i)^gross profit$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/gross-profit])
  ;; => #:yf{:gross-profit 9.694E10}
  )

(pco/defresolver ebitda [{:yf.key-stats.page/keys [htree]}]
  {:yf/ebitda
   (->> htree
        (find-value-by-description #"(?i)^ebitda$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/ebitda])
  ;; => #:yf{:ebitda 7.169E10}
  )

(pco/defresolver net-income-avi-to-common [{:yf.key-stats.page/keys [htree]}]
  {:yf/net-income-avi-to-common
   (->> htree
        (find-value-by-description #"(?i)^net income avi to common$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/net-income-avi-to-common])
  ;; => #:yf{:net-income-avi-to-common 5.131E10}
  )

(pco/defresolver diluted-eps [{:yf.key-stats.page/keys [htree]}]
  {:yf/diluted-eps
   (->> htree
        (find-value-by-description #"(?i)^diluted eps")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/diluted-eps])
  ;; => #:yf{:diluted-eps 6.71}
  )

(pco/defresolver total-cash [{:yf.key-stats.page/keys [htree]}]
  {:yf/total-cash
   (->> htree
        (find-value-by-description #"(?i)^total cash$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/total-cash])
  ;; => #:yf{:total-cash 1.3199000000000002E11}
  )

(pco/defresolver quarterly-earnings-growth [{:yf.key-stats.page/keys [htree]}]
  {:yf/quarterly-earnings-growth
   (->> htree
        (find-value-by-description #"(?i)^quarterly earnings growth$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/quarterly-earnings-growth])
  ;; => #:yf{:quarterly-earnings-growth 0.327}
  )

(pco/defresolver total-cash-per-share [{:yf.key-stats.page/keys [htree]}]
  {:yf/total-cash-per-share
   (->> htree
        (find-value-by-description #"(?i)^total cash per share$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/total-cash-per-share])
  ;; => #:yf{:total-cash-per-share 17.5}
  )

(pco/defresolver total-debt [{:yf.key-stats.page/keys [htree]}]
  {:yf/total-debt
   (->> htree
        (find-value-by-description #"(?i)^total debt$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/total-debt])
  ;; => #:yf{:total-debt 8.278E10}
  )

(pco/defresolver total-debt-to-equity [{:yf.key-stats.page/keys [htree]}]
  {:yf/total-debt-to-equity
   (->> htree
        (find-value-by-description #"(?i)^total debt/equity$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/total-debt-to-equity])
  ;; => #:yf{:total-debt-to-equity 63.56}
  )

(pco/defresolver current-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/current-ratio
   (->> htree
        (find-value-by-description #"(?i)^current ratio$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/current-ratio])
  ;; => #:yf{:current-ratio 2.58}
  )

(pco/defresolver book-value-per-share [{:yf.key-stats.page/keys [htree]}]
  {:yf/book-value-per-share
   (->> htree
        (find-value-by-description #"(?i)^book value per share$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/book-value-per-share])
  ;; => #:yf{:book-value-per-share 17.26}
  )

(pco/defresolver operating-cash-flow [{:yf.key-stats.page/keys [htree]}]
  {:yf/operating-cash-flow
   (->> htree
        (find-value-by-description #"(?i)^operating cash flow$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/operating-cash-flow])
  ;; => #:yf{:operating-cash-flow 6.803E10}
  )

(pco/defresolver levered-free-cash-flow [{:yf.key-stats.page/keys [htree]}]
  {:yf/levered-free-cash-flow
   (->> htree
        (find-value-by-description #"(?i)^levered free cash flow$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/levered-free-cash-flow])
  ;; => #:yf{:levered-free-cash-flow 3.479E10}
  )

(pco/defresolver beta [{:yf.key-stats.page/keys [htree]}]
  {:yf/beta
   (->> htree
        (find-value-by-description #"(?i)^beta \(5y monthly\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/beta])
  ;; => #:yf{:beta 0.81}
  )

(pco/defresolver week-change-52 [{:yf.key-stats.page/keys [htree]}]
  {:yf/week-change-52
   (->> htree
        (find-value-by-description #"(?i)^52\-week change$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/week-change-52])
  ;; => #:yf{:week-change-52 0.5376}
  )

(pco/defresolver s&p500-week-change-52 [{:yf.key-stats.page/keys [htree]}]
  {:yf/s&p500-week-change-52
   (->> htree
        (find-value-by-description #"(?i)^s&p500 52\-week change$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/s&p500-week-change-52])
  ;; => #:yf{:s&p500-week-change-52 0.39880000000000004}
  )

(pco/defresolver week-high-52 [{:yf.key-stats.page/keys [htree]}]
  {:yf/week-high-52
   (->> htree
        (find-value-by-description #"(?i)^52 week high$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/week-high-52])
  ;; => #:yf{:week-high-52 246.13}
  )

(pco/defresolver week-low-52 [{:yf.key-stats.page/keys [htree]}]
  {:yf/week-low-52
   (->> htree
        (find-value-by-description #"(?i)^52 week low$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/week-low-52])
  ;; => #:yf{:week-low-52 132.52}
  )

(pco/defresolver day-ma-50 [{:yf.key-stats.page/keys [htree]}]
  {:yf/day-ma-50
   (->> htree
        (find-value-by-description #"(?i)^50\-day moving average$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/day-ma-50])
  ;; => #:yf{:day-ma-50 235.62}
  )

(pco/defresolver day-ma-200 [{:yf.key-stats.page/keys [htree]}]
  {:yf/day-ma-200
   (->> htree
        (find-value-by-description #"(?i)^200\-day moving average$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/day-ma-200])
  ;; => #:yf{:day-ma-200 219.43}
  )

(pco/defresolver avg-vol-3-month [{:yf.key-stats.page/keys [htree]}]
  {:yf/avg-vol-3-month
   (->> htree
        (find-value-by-description #"(?i)^avg vol \(3 month\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/avg-vol-3-month])
  ;; => #:yf{:avg-vol-3-month 2.955E7}
  )

(pco/defresolver avg-vol-10-day [{:yf.key-stats.page/keys [htree]}]
  {:yf/avg-vol-10-day
   (->> htree
        (find-value-by-description #"(?i)^avg vol \(10 day\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/avg-vol-10-day])
  ;; => #:yf{:avg-vol-10-day 3.438E7}
  )

(pco/defresolver shares-outstanding [{:yf.key-stats.page/keys [htree]}]
  {:yf/shares-outstanding
   (->> htree
        (find-value-by-description #"(?i)^shares outstanding$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/shares-outstanding])
  ;; => #:yf{:shares-outstanding 7.56E9}
  )

(pco/defresolver float [{:yf.key-stats.page/keys [htree]}]
  {:yf/float
   (->> htree
        (find-value-by-description #"(?i)^float$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/float])
  ;; => #:yf{:float 7.43E9}
  )

(pco/defresolver percent-held-by-insiders [{:yf.key-stats.page/keys [htree]}]
  {:yf/percent-held-by-insiders
   (->> htree
        (find-value-by-description #"(?i)^\% held by insiders$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/percent-held-by-insiders])
  ;; => #:yf{:percent-held-by-insiders 6.0E-4}
  )

(pco/defresolver percent-held-by-institutions [{:yf.key-stats.page/keys [htree]}]
  {:yf/percent-held-by-institutions
   (->> htree
        (find-value-by-description #"(?i)^\% held by institutions$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/percent-held-by-institutions])
  ;; => #:yf{:percent-held-by-institutions 0.7184}
  )

(pco/defresolver shares-short [{:yf.key-stats.page/keys [htree]}]
  {:yf/shares-short
   (->> htree
        (find-value-by-description #"(?i)^shares short \(.+\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/shares-short])
  ;; => #:yf{:shares-short 4.06E7}
  )

(pco/defresolver short-ratio [{:yf.key-stats.page/keys [htree]}]
  {:yf/short-ratio
   (->> htree
        (find-value-by-description #"(?i)^short ratio \(.+\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/short-ratio])
  ;; => #:yf{:short-ratio 1.32}
  )

(pco/defresolver short-percent-of-float [{:yf.key-stats.page/keys [htree]}]
  {:yf/short-percent-of-float
   (->> htree
        (find-value-by-description #"(?i)^short \% of float \(.+\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/short-percent-of-float])
  ;; => #:yf{:short-percent-of-float 0.0054}
  )

(pco/defresolver short-percent-of-shares-outstanding [{:yf.key-stats.page/keys [htree]}]
  {:yf/short-percent-of-shares-outstanding
   (->> htree
        (find-value-by-description #"(?i)^short \% of shares outstanding \(.+\)$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/short-percent-of-shares-outstanding])
  ;; => #:yf{:short-percent-of-shares-outstanding 0.0054}
  )

(pco/defresolver forward-annual-dividend-rate [{:yf.key-stats.page/keys [htree]}]
  {:yf/forward-annual-dividend-rate
   (->> htree
        (find-value-by-description #"(?i)^forward annual dividend rate$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/forward-annual-dividend-rate])
  ;; => #:yf{:forward-annual-dividend-rate 2.24}
  )

(pco/defresolver forward-annual-dividend-yield [{:yf.key-stats.page/keys [htree]}]
  {:yf/forward-annual-dividend-yield
   (->> htree
        (find-value-by-description #"(?i)^forward annual dividend yield$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/forward-annual-dividend-yield])
  ;; => #:yf{:forward-annual-dividend-yield 0.0097}
  )

(pco/defresolver trailing-annual-dividend-rate [{:yf.key-stats.page/keys [htree]}]
  {:yf/trailing-annual-dividend-rate
   (->> htree
        (find-value-by-description #"(?i)^trailing annual dividend rate$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/trailing-annual-dividend-rate])
  ;; => #:yf{:trailing-annual-dividend-rate 2.14}
  )

(pco/defresolver trailing-annual-dividend-yield [{:yf.key-stats.page/keys [htree]}]
  {:yf/trailing-annual-dividend-yield
   (->> htree
        (find-value-by-description #"(?i)^trailing annual dividend yield$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/trailing-annual-dividend-yield])
  ;; => #:yf{:trailing-annual-dividend-yield 0.0092}
  )

(pco/defresolver five-year-average-dividend-yield  [{:yf.key-stats.page/keys [htree]}]
  {:yf/five-year-average-dividend-yield
   (->> htree
        (find-value-by-description #"(?i)^5 year average dividend yield$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/five-year-average-dividend-yield])
  ;; => #:yf{:five-year-average-dividend-yield 1.68}
  )

(pco/defresolver payout-ratio  [{:yf.key-stats.page/keys [htree]}]
  {:yf/payout-ratio
   (->> htree
        (find-value-by-description #"(?i)^payout ratio$")
        (->value))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/payout-ratio])
  ;; => #:yf{:payout-ratio 0.3115}
  )

(pco/defresolver dividend-date  [{:yf.key-stats.page/keys [htree]}]
  {:yf/dividend-date
   (when-let [s (->> htree
                     (find-value-by-description #"(?i)^dividend date$"))]
     (when (string? s)
       (.format
        (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
        (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s))))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/dividend-date])
  ;; => #:yf{:dividend-date "2021-03-10"}
  )

(pco/defresolver ex-dividend-date  [{:yf.key-stats.page/keys [htree]}]
  {:yf/ex-dividend-date
   (when-let [s (->> htree
                     (find-value-by-description #"(?i)^ex-dividend date$"))]
     (when (string? s)
       (.format
        (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
        (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s))))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/ex-dividend-date])
  ;; => #:yf{:ex-dividend-date "2021-02-16"}
  )

(pco/defresolver profile-page-htree
  [{:yf/keys [ticker]}]
  {::pco/cache-store ::ttl-cache}
  {:yf.profile-page/htree
   (let [resp (http/get (format "https://finance.yahoo.com/quote/%s/profile" (str/upper-case ticker))
                        {:cookie-handler cookie-handler})]
     (when-not (seq (:trace-redirects resp))
       (-> resp
           :body
           hc/parse
           hc/as-hickory)))})

(pco/defresolver company-name [{:yf.profile-page/keys [htree]}]
  {:yf/company-name
   (some->> htree
            (hs/select (hs/descendant
                        (hs/attr :data-test #(= % "asset-profile"))
                        (hs/tag :h3)))
            first
            :content
            first
            str/lower-case)})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf.company/name])
  ;; => #:yf.company{:name "microsoft corporation"}
  )

(pco/defresolver company-address [{:yf.profile-page/keys [htree]}]
  {::pco/output [{:yf/company-address
                  [:yf.company.address/street
                   :yf.company.address/city
                   :yf.company.address/country]}]}
  {:yf.company/address
   (some->> htree
            (hs/select (hs/descendant
                        (hs/attr :data-test #(= % "asset-profile"))
                        (hs/tag :div)
                        (hs/tag :p)))
            first
            :content
            (filter string?)
            ((fn [[street city country]]
               {:yf.company.address/street  (some-> street  (str/lower-case))
                :yf.company.address/city    (some-> city    (str/lower-case))
                :yf.company.address/country (some-> country (str/lower-case))})))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [{:yf.company/address [:yf.company.address/city]}])
  ;; =>
  ;; #:yf.company{:address #:yf.company.address{:city "redmond, wa 98052-6399"}}
  )

(pco/defresolver company-profile [{:yf.profile-page/keys [htree]}]
  {:yf/company-profile
   (some->> htree
            (hs/select (hs/descendant
                        (hs/attr :data-test #(= % "asset-profile"))
                        (hs/tag :div)
                        (hs/nth-of-type 2 :p)))
            first
            :content
            (mapv :content)
            ((fn [{[sector _] 4 [industry _] 10 [employees _] 16}]
               {:yf/sector    (some-> sector (str/lower-case))
                :yf/industry  (some-> industry (str/lower-case))
                :yf/employees (some-> employees :content first (str/replace "," "") (e/as-?float))})))})

(pco/defresolver company-profile* [{:yf/keys [company-profile]}]
  {::pco/output [:yf/sector
                 :yf/industry
                 :yf/employees]}
  company-profile)

(comment
  (p.eql/process env {:yf/ticker "msft"} [{:yf.company/profile [:yf.company.profile/sector]}])
  ;; => #:yf.company{:profile #:yf.company.profile{:sector "technology"}}
  )

(pco/defresolver ticker-name [{:yf/keys [ticker]}]
  {:yf/ticker-name
   (some->> (http/get (str "https://finance.yahoo.com/quote/" (str/upper-case ticker)))
            :body
            (hc/parse)
            (hc/as-hickory)
            (hs/select (hs/descendant
                        (hs/id "quote-header-info")
                        (hs/tag :h1)))
            first
            :content
            first
            str/lower-case
            ((fn [s] (-> s (str/split #"-") second (str/trim)))))})

(comment
  (p.eql/process env {:yf/ticker "msft"} [:yf/ticker-name])
  ;; => #:yf{:ticker-name "microsoft corporation"}
  )

(def env
  (-> {::ttl-cache ttl-cache_}
      (pci/register
       [(pbir/alias-resolver :ticker :yf/ticker)
        quotes
        quote-page-htree
        key-stats-htree
        market-cap
        enterprise-value
        trailing-pe-ratio
        forward-pe-ratio
        peg-ratio
        price-to-sales-ratio
        price-to-book-ratio
        enterprise-value-to-revenue-ratio
        enterprise-value-to-ebitda-ratio
        fiscal-year-ends
        most-recent-quarter
        profit-margin
        operating-margin
        roa
        roe
        revenue
        revenue-per-share
        quarterly-revenue-growth
        gross-profit
        ebitda
        net-income-avi-to-common
        diluted-eps
        total-cash
        quarterly-earnings-growth
        total-cash-per-share
        total-debt
        total-debt-to-equity
        current-ratio
        book-value-per-share
        operating-cash-flow
        levered-free-cash-flow
        beta
        week-change-52
        s&p500-week-change-52
        week-high-52
        week-low-52
        day-ma-50
        day-ma-200
        avg-vol-3-month
        avg-vol-10-day
        shares-outstanding
        float
        percent-held-by-insiders
        percent-held-by-institutions
        shares-short
        short-ratio
        short-percent-of-float
        short-percent-of-shares-outstanding
        forward-annual-dividend-rate
        forward-annual-dividend-yield
        trailing-annual-dividend-rate
        trailing-annual-dividend-yield
        five-year-average-dividend-yield
        payout-ratio
        dividend-date
        ex-dividend-date
        profile-page-htree
        company-name
        company-address
        company-profile
        company-profile*
        ticker-name])))

(comment
  (p.eql/process env {:ticker "jd"} [:yf/quotes])
  (get-data "jd")
  (get-crumb "jd"))
