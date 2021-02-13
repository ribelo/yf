(ns ribelo.yf
  (:refer-clojure :exclude [float])
  (:require
   [clojure.string]
   [taoensso.encore :as e]
   [hato.client :as http]
   [hickory.core :as hc]
   [hickory.select :as hs]
   [cuerdas.core :as str]
   [ribelo.pathos :as p]))

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

(defn- str-time->epoch [s]
  (quot (.getTime (.parse (e/simple-date-format "yyyy-MM-dd" :timezone :utc) s)) 1000))

(def ^:private memoized-http-get
  (e/memoize
      (fn
        ([url opts] (http/get url opts))
        ([url] (memoized-http-get url {})))))

(defn- get-crumb [symbol]
  (try-times {:max-retries 5 :sleep 3000}
    (let [url     "https://finance.yahoo.com/quote/%s/history"
          resp    (http/get (format url (str/upper (name symbol))))
          cookies (:cookies resp)
          body    (:body resp)
          crumb   (second (re-find #"\"CrumbStore\":\{\"crumb\":\"(.{11})\"\}" body))]
      (if crumb
        [cookies crumb]
        (throw (ex-info "empty response" {:symbol symbol}))))))

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
                            (str/upper (name ticker))
                            start-time*
                            end-time*
                            interval
                            event
                            crumb))
          :body
          str/lines
          (into []
                (comp
                 (drop 1)
                 (map #(str/split % #","))
                 (map (fn [[date open high low close _ volume]]
                        {:date   date
                         :open   (e/as-?float open)
                         :high   (e/as-?float high)
                         :low    (e/as-?float low)
                         :close  (e/as-?float close)
                         :volume (e/as-?int volume)}))
                 (map (partial p/add-ns-to-map :yf.quotes))
                 (filter (fn [m] (every? identity (vals m)))))))
     (throw (ex-info "empty response" {:symbol ticker})))))

(p/reg-resolver ::quotes
  [{:yf/keys [ticker start-time end-time]}]
  {::p/memoize [(e/ms :mins 5)]
   ::p/input   [:yf/ticker]
   ::p/output  [{:yf/quotes [:yf.quotes/date :yf.quotes/open :yf.quotes/high
                             :yf.quotes/low :yf.quotes/close :yf.quotes/volume]}]}
  (println ticker start-time end-time)
  {:yf/quotes
   (get-data ticker (e/assoc-some {}
                                  :start-time start-time
                                  :end-time   end-time))})
(comment
  (p/eql {:yf/ticker :msft
          :yf/start-time "2020-01-02"} [:yf/quotes]))


(defn stock? [symbol]
  (->> (memoized-http-get (str "https://finance.yahoo.com/quote/" (str/upper symbol)))
       :body
       (hc/parse)
       (hc/as-hickory)
       (hs/select (hs/child
                   (hs/attr "data-test" #(= % "MARKET_CAP-value"))
                   (hs/tag :span)))
       not-empty
       nil?
       not))

(comment
  (stock? "msft"))

(defn etf? [symbol]
  (not (stock? symbol)))

(comment
  (etf? "msft"))

(def ^:private get-key-stats-htree
  (e/memoize
      (fn [symbol]
        (let [resp (memoized-http-get (format "https://finance.yahoo.com/quote/%s/key-statistics" (str/upper symbol)))]
          (when-not (seq (:trace-redirects resp))
            (-> resp
                :body
                hc/parse
                hc/as-hickory))))))

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
        (* num (unit->value (str/lower unit) 1.0))))))

(defn- find-value-by-description [re htree]
  (->> htree
       (hs/select (hs/follow
                   (hs/has-child (hs/find-in-text re))
                   (hs/tag :td)))
       first
       :content
       first))

(defn market-cap [ticker]
  (->> (get-key-stats-htree ticker)
       (find-value-by-description #"(?i)^market cap")
       (->value)))

(p/reg-resolver ::market-cap
  [{:yf/keys [ticker]}]
  {:yf/market-cap (market-cap ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/market-cap]))

(defn enterprise-value [ticker]
  (->> (get-key-stats-htree ticker)
       (find-value-by-description #"(?i)^enterprise value")
       (->value)))

(p/reg-resolver ::enterprise-value
  [{:yf/keys [ticker]}]
  {:yf/enterprise-value (enterprise-value ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/enterprise-value]))

(defn trailing-pe-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing p/e")
       (->value)))

(p/reg-resolver ::trailing-pe-ratio
  [{:yf/keys [ticker]}]
  {:yf/trailing-pe-ratio (trailing-pe-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/trailing-pe-ratio]))

(defn forward-pe-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward p/e")
       (->value)))

(p/reg-resolver ::forward-pe-ratio
  [{:yf/keys [ticker]}]
  {:yf/forward-pe-ratio (forward-pe-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/forward-pe-ratio]))

(defn peg-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^peg ratio")
       (->value)))

(p/reg-resolver ::peg-ratio
  [{:yf/keys [ticker]}]
  {:yf/peg-ratio (peg-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/peg-ratio]))

(defn price-to-sales-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^price/sales")
       (->value)))

(p/reg-resolver ::price-to-sales-ratio
  [{:yf/keys [ticker]}]
  {:yf/price-to-sales-ratio (price-to-sales-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/price-to-sales-ratio]))

(defn price-to-book-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value")
       (->value)))

(p/reg-resolver ::price-to-book-ratio
  [{:yf/keys [ticker]}]
  {:yf/price-to-book-ratio (price-to-book-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/price-to-book-ratio]))

(defn enterprise-value-to-revenue-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value/revenue")
       (->value)))

(p/reg-resolver ::enterprise-value-to-revenue-ratio
  [{:yf/keys [ticker]}]
  {:yf/enterprise-value-to-revenue-ratio (enterprise-value-to-revenue-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/enterprise-value-to-revenue-ratio]))

(defn enterprise-value-to-ebitda-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value/ebitda")
       (->value)))

(p/reg-resolver ::enterprise-value-to-ebitda-ratio
  [{:yf/keys [ticker]}]
  {:yf/enterprise-value-to-ebitda-ratio (enterprise-value-to-ebitda-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/enterprise-value-to-ebitda-ratio]))

(defn fiscal-year-ends [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^fiscal year ends"))]
    (when (string? s)
      (.format
       (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
       (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s)))))

(p/reg-resolver ::fiscal-year-ends
  [{:yf/keys [ticker]}]
  {:yf/fiscal-year-ends (fiscal-year-ends ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/fiscal-year-ends]))

(defn most-recent-quarter [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^most recent quarter"))]
    (when (string? s)
      (.format
       (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
       (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s)))))

(p/reg-resolver ::most-recent-quarter
  [{:yf/keys [ticker]}]
  {:yf/most-recent-quarter (most-recent-quarter ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/most-recent-quarter]))

(defn profit-margin [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^profit margin")
       (->value)))

(p/reg-resolver ::profit-margin
  [{:yf/keys [ticker]}]
  {:yf/profit-margin (profit-margin ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/profit-margin]))

(defn operating-margin [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^operating margin")
       (->value)))

(p/reg-resolver ::operating-margin
  [{:yf/keys [ticker]}]
  {:yf/operating-margin (operating-margin ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/operating-margin]))

(defn roa [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^return on assets")
       (->value)))

(p/reg-resolver ::roa
  [{:yf/keys [ticker]}]
  {:yf/roa (roa ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/roa]))

(defn roe [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^return on equity")
       (->value)))

(p/reg-resolver ::roe
  [{:yf/keys [ticker]}]
  {:yf/roe (roe ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/roe]))

(defn revenue [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^revenue$")
       (->value)))

(defn revenue-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^revenue per share")
       (->value)))

(p/reg-resolver ::revenue-per-share
  [{:yf/keys [ticker]}]
  {:yf/revenue-per-share (revenue-per-share ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/revenue-per-share]))

(p/reg-resolver ::revenue
  [{:yf/keys [ticker]}]
  {:yf/revenue (revenue ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/revenue]))

(comment
  (revenue-per-share "msft")
  ;; => 20.23
  )

(defn quarterly-revenue-growth [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^quarterly revenue growth$")
       (->value)))

(p/reg-resolver ::quarterly-revenue-growth
  [{:yf/keys [ticker]}]
  {:yf/quarterly-revenue-growth (quarterly-revenue-growth ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/quarterly-revenue-growth]))

(defn gross-profit [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^gross profit$")
       (->value)))

(p/reg-resolver ::gross-profit
  [{:yf/keys [ticker]}]
  {:yf/gross-profit (gross-profit ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/gross-profit]))

(defn ebitda [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^ebitda$")
       (->value)))

(p/reg-resolver ::ebitda
  [{:yf/keys [ticker]}]
  {:yf/ebitda (ebitda ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/ebitda]))

(defn net-income-avi-to-common [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^net income avi to common$")
       (->value)))

(p/reg-resolver ::net-income-avi-to-common
  [{:yf/keys [ticker]}]
  {:yf/net-income-avi-to-common (net-income-avi-to-common ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/net-income-avi-to-common]))

(defn diluted-eps [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^diluted eps")
       (->value)))

(p/reg-resolver ::diluted-eps
  [{:yf/keys [ticker]}]
  {:yf/diluted-eps (diluted-eps ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/diluted-eps]))

(defn total-cash [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total cash$")
       (->value)))

(p/reg-resolver ::total-cash
  [{:yf/keys [ticker]}]
  {:yf/total-cash (total-cash ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/total-cash]))

(defn quarterly-earnings-growth [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^quarterly earnings growth$")
       (->value)))

(p/reg-resolver ::quarterly-earnings-growth
  [{:yf/keys [ticker]}]
  {:yf/quarterly-earnings-growth (quarterly-earnings-growth ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/quarterly-earnings-growth]))

(defn total-cash-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total cash per share$")
       (->value)))

(p/reg-resolver ::total-cash-per-share
  [{:yf/keys [ticker]}]
  {:yf/total-cash-per-share (total-cash-per-share ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/total-cash-per-share]))

(defn total-debt [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total debt$")
       (->value)))

(p/reg-resolver ::total-debt
  [{:yf/keys [ticker]}]
  {:yf/total-debt (total-debt ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/total-debt]))

(defn total-debt-to-equity [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total debt/equity$")
       (->value)))

(p/reg-resolver ::total-debt-to-equity
  [{:yf/keys [ticker]}]
  {:yf/total-debt-to-equity (total-debt-to-equity ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/total-debt-to-equity]))

(defn current-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^current ratio$")
       (->value)))

(p/reg-resolver ::current-ratio
  [{:yf/keys [ticker]}]
  {:yf/current-ratio (current-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/current-ratio]))

(defn book-value-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^book value per share$")
       (->value)))

(p/reg-resolver ::book-value-per-share
  [{:yf/keys [ticker]}]
  {:yf/book-value-per-share (book-value-per-share ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/book-value-per-share]))

(defn operating-cash-flow [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^operating cash flow$")
       (->value)))

(p/reg-resolver ::operating-cash-flow
  [{:yf/keys [ticker]}]
  {:yf/operating-cash-flow (operating-cash-flow ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/operating-cash-flow]))

(defn levered-free-cash-flow [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^levered free cash flow$")
       (->value)))

(p/reg-resolver ::levered-free-cash-flow
  [{:yf/keys [ticker]}]
  {:yf/levered-free-cash-flow (levered-free-cash-flow ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/levered-free-cash-flow]))

(defn beta [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^beta \(5y monthly\)$")
       (->value)))

(p/reg-resolver ::beta
  [{:yf/keys [ticker]}]
  {:yf/beta (beta ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/beta]))

(defn week-change-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52\-week change$")
       (->value)))

(p/reg-resolver ::week-change-52
  [{:yf/keys [ticker]}]
  {:yf/week-change-52 (week-change-52 ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/week-change-52]))

(defn s&p500-week-change-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^s&p500 52\-week change$")
       (->value)))

(p/reg-resolver ::s&p500-week-change-52
  [{:yf/keys [ticker]}]
  {:yf/s&p500-week-change-52 (s&p500-week-change-52 ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/s&p500-week-change-52]))

(defn week-high-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52 week high$")
       (->value)))

(p/reg-resolver ::week-high-52
  [{:yf/keys [ticker]}]
  {:yf/week-high-52 (week-high-52 ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/week-high-52]))

(defn week-low-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52 week low$")
       (->value)))

(p/reg-resolver ::week-low-52
  [{:yf/keys [ticker]}]
  {:yf/week-low-52 (week-low-52 ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/week-low-52]))

(defn day-ma-50 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^50\-day moving average$")
       (->value)))

(p/reg-resolver ::day-ma-50
  [{:yf/keys [ticker]}]
  {:yf/day-ma-50 (day-ma-50 ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/day-ma-50]))

(defn day-ma-200 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^200\-day moving average$")
       (->value)))

(p/reg-resolver ::day-ma-200
  [{:yf/keys [ticker]}]
  {:yf/day-ma-200 (day-ma-200 ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/day-ma-200]))

(defn avg-vol-3-month [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^avg vol \(3 month\)$")
       (->value)))

(p/reg-resolver ::avg-vol-3-month
  [{:yf/keys [ticker]}]
  {:yf/avg-vol-3-month (avg-vol-3-month ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/avg-vol-3-month]))

(defn avg-vol-10-day [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^avg vol \(10 day\)$")
       (->value)))

(p/reg-resolver ::avg-vol-10-day
  [{:yf/keys [ticker]}]
  {:yf/avg-vol-10-day (avg-vol-10-day ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/avg-vol-10-day]))

(defn shares-outstanding [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^shares outstanding$")
       (->value)))

(p/reg-resolver ::shares-outstanding
  [{:yf/keys [ticker]}]
  {:yf/shares-outstanding (shares-outstanding ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/shares-outstanding]))

(defn float [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^float$")
       (->value)))

(p/reg-resolver ::float
  [{:yf/keys [ticker]}]
  {:yf/float (float ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/float]))

(defn percent-held-by-insiders [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^\% held by insiders$")
       (->value)))

(p/reg-resolver ::percent-held-by-insiders
  [{:yf/keys [ticker]}]
  {:yf/percent-held-by-insiders (percent-held-by-insiders ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/percent-held-by-insiders]))

(defn percent-held-by-institutions [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^\% held by institutions$")
       (->value)))

(p/reg-resolver ::percent-held-by-institutions
  [{:yf/keys [ticker]}]
  {:yf/percent-held-by-institutions (percent-held-by-institutions ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/percent-held-by-institutions]))

(defn shares-short [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^shares short \(.+\)$")
       (->value)))

(p/reg-resolver ::shares-short
  [{:yf/keys [ticker]}]
  {:yf/shares-short (shares-short ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/shares-short]))

(defn short-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short ratio \(.+\)$")
       (->value)))

(p/reg-resolver ::short-ratio
  [{:yf/keys [ticker]}]
  {:yf/short-ratio (short-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/short-ratio]))

(defn short-percent-of-float [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short \% of float \(.+\)$")
       (->value)))

(p/reg-resolver ::short-percent-of-float
  [{:yf/keys [ticker]}]
  {:yf/short-percent-of-float (short-percent-of-float ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/short-percent-of-float]))

(defn short-percent-of-shares-outstanding [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short \% of shares outstanding \(.+\)$")
       (->value)))

(p/reg-resolver ::short-percent-of-shares-outstanding
  [{:yf/keys [ticker]}]
  {:yf/short-percent-of-shares-outstanding (short-percent-of-shares-outstanding ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/short-percent-of-shares-outstanding]))

(defn forward-annual-dividend-rate [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward annual dividend rate$")
       (->value)))

(p/reg-resolver ::forward-annual-dividend-rate
  [{:yf/keys [ticker]}]
  {:yf/forward-annual-dividend-rate (forward-annual-dividend-rate ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/forward-annual-dividend-rate]))

(defn forward-annual-dividend-yield [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward annual dividend yield$")
       (->value)))

(p/reg-resolver ::forward-annual-dividend-yield
  [{:yf/keys [ticker]}]
  {:yf/forward-annual-dividend-yield (forward-annual-dividend-yield ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/forward-annual-dividend-yield]))

(defn trailing-annual-dividend-rate [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing annual dividend rate$")
       (->value)))

(p/reg-resolver ::trailing-annual-dividend-rate
  [{:yf/keys [ticker]}]
  {:yf/trailing-annual-dividend-rate (trailing-annual-dividend-rate ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/trailing-annual-dividend-rate]))

(defn trailing-annual-dividend-yield [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing annual dividend yield$")
       (->value)))

(p/reg-resolver ::trailing-annual-dividend-yield
  [{:yf/keys [ticker]}]
  {:yf/trailing-annual-dividend-yield (trailing-annual-dividend-yield ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/trailing-annual-dividend-yield]))

(defn five-year-average-dividend-yield  [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^5 year average dividend yield$")
       (->value)))

(p/reg-resolver ::five-year-average-dividend-yield
  [{:yf/keys [ticker]}]
  {:yf/five-year-average-dividend-yield (five-year-average-dividend-yield ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/five-year-average-dividend-yield]))

(defn payout-ratio  [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^payout ratio$")
       (->value)))

(p/reg-resolver ::payout-ratio
  [{:yf/keys [ticker]}]
  {:yf/payout-ratio (payout-ratio ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/payout-ratio]))

(defn dividend-date  [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^dividend date$"))]
    (when (string? s)
      (.format
       (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
       (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s)))))

(p/reg-resolver ::dividend-date
  [{:yf/keys [ticker]}]
  {:yf/dividend-date (dividend-date ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/dividend-date]))

(defn ex-dividend-date  [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^ex-dividend date$"))]
    (when (string? s)
      (.format
       (e/simple-date-format "yyyy-MM-dd" :timezone :utc)
       (.parse (e/simple-date-format "LLL dd, yyyy" :timezone :utc) s)))))

(p/reg-resolver ::ex-dividend-date
  [{:yf/keys [ticker]}]
  {:yf/ex-dividend-date (ex-dividend-date ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/ex-dividend-date]))

(def ^:private get-profile-htree
  (e/memoize
      (fn [ticker]
        (let [resp (memoized-http-get (format "https://finance.yahoo.com/quote/%s/profile" (str/upper ticker)))]
          (when-not (seq (:trace-redirects resp))
            (-> resp
                :body
                hc/parse
                hc/as-hickory))))))

(defn company-name [ticker]
  (when (stock? ticker)
    (->> (get-profile-htree ticker)
         (hs/select (hs/descendant
                     (hs/attr :data-test #(= % "asset-profile"))
                     (hs/tag :h3)))
         first
         :content
         first
         str/lower)))

(p/reg-resolver ::company-name
  [{:yf/keys [ticker]}]
  {:yf.company/name (company-name ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf.company/name]))

(defn company-address [ticker]
  (when (stock? ticker)
    (->> (get-profile-htree ticker)
         (hs/select (hs/descendant
                     (hs/attr :data-test #(= % "asset-profile"))
                     (hs/tag :div)
                     (hs/tag :p)))
         first
         :content
         (filter string?)
         ((fn [[street city country]]
            {:yf.company.address/street  (some-> street (str/lower))
             :yf.company.address/city    (some-> city (str/lower))
             :yf.company.address/country (some-> country (str/lower))})))))

(p/reg-resolver ::company-address
  [{:yf/keys [ticker]}]
  {::p/output [:yf.company.address/street
               :yf.company.address/city
               :yf.company.address/country]}
  (company-address ticker))

(comment
  (p/eql {:yf/ticker "msft"} [:yf.company.address/street]))

(defn company-profile [symbol]
  (->> (get-profile-htree (str/upper symbol))
       (hs/select (hs/descendant
                   (hs/attr :data-test #(= % "asset-profile"))
                   (hs/tag :div)
                   (hs/nth-of-type 2 :p)))
       first
       :content
       (mapv :content)
       ((fn [{[sector _] 4 [industry _] 10 [employees _] 16}]
          {:yf.company/sector    (some-> sector (str/lower))
           :yf.company/industry  (some-> industry (str/lower))
           :yf.company/employees (some-> employees :content first (str/replace "," "") (e/as-?float))}))))

(p/reg-resolver ::company-profile
  [{:yf/keys [ticker]}]
  {::p/output [:yf.company/sector
               :yf.company/industry
               :yf.company/employees]}
  (company-profile ticker))

(comment
  (p/eql {:yf/ticker "msft"} [:yf.company/sector]))

(defn ticker-name [ticker]
  (->> (memoized-http-get (str "https://finance.yahoo.com/quote/" (str/upper ticker)))
       :body
       (hc/parse)
       (hc/as-hickory)
       (hs/select (hs/descendant
                   (hs/id "quote-header-info")
                   (hs/tag :h1)))
       first
       :content
       first
       str/lower
       ((fn [s] (-> s (str/split #"-") second (str/trim))))))

(p/reg-resolver ::ticker-name
  [{:yf/keys [ticker]}]
  {:yf/ticker-name (ticker-name ticker)})

(comment
  (p/eql {:yf/ticker "msft"} [:yf/ticker-name]))
