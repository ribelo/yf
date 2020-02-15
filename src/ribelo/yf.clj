(ns ribelo.yf
  (:refer-clojure :exclude [float])
  (:require
   [taoensso.encore :as e]
   [clj-http.client :as http]
   [clojure.string :as str]
   [java-time :as jt]
   [hickory.core :as hc]
   [hickory.select :as hs]))

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

(defn str-time->epoch [s]
  (quot (jt/to-millis-from-epoch (jt/java-date (jt/local-date s) (jt/zone-offset 0))) 1000))

(def memoized-http-get
  (e/memoize
   (fn
     ([url opts] (http/get url opts))
     ([url] (memoized-http-get url {})))))

(defn get-cookie&crumb [symbol]
  (try-times {:max-retries 5 :sleep 3000}
             (let [url     "https://finance.yahoo.com/quote/%s/history"
                   resp    (http/get (format url (str/upper-case (name symbol))))
                   cookies (:cookies resp)
                   body    (:body resp)
                   crumb   (second (re-find #"\"CrumbStore\":\{\"crumb\":\"(.{11})\"\}" body))]
               (if crumb
                 [cookies crumb]
                 (throw (ex-info "empty response" {:symbol symbol}))))))

(defn get-data
  ([symbol] (get-data symbol {}))
  ([symbol {:keys [start end interval]
            :or   {start    "2000-01-01"
                   end      (jt/format (jt/local-date))
                   interval "1d"}}]
   (if-let [[cookies crumb] (get-cookie&crumb symbol)]
     (let [url        "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s"
           start-time (str-time->epoch start)
           end-time   (str-time->epoch end)
           event      "history"]
       (->> (http/get (format url
                              (str/upper-case (name symbol))
                              start-time
                              end-time
                              interval
                              event
                              crumb)
                      {:cookies cookies})
            :body
            str/split-lines
            (into []
                  (comp
                   (drop 1)
                   (map #(str/split % #","))
                   (map (fn [[date open high low close _ volume]]
                          {:date   (jt/local-date date)
                           :open   (e/as-?float open)
                           :high   (e/as-?float high)
                           :low    (e/as-?float low)
                           :close  (e/as-?float close)
                           :volume (e/as-?float volume)}))
                   (filter (fn [m] (every? identity (vals m))))))))
     (throw (ex-info "empty response" {:symbol symbol})))))

(defn stock? [symbol]
  (->> (memoized-http-get (str "https://finance.yahoo.com/quote/" (str/upper-case symbol)))
       :body
       (hc/parse)
       (hc/as-hickory)
       (hs/select (hs/child
                   (hs/attr "data-test" #(= % "MARKET_CAP-value"))
                   (hs/tag :span)))
       not-empty
       nil?
       not))

(defn etf? [symbol]
  (not (stock? symbol)))

(def ^:private get-key-stats-htree
  (e/memoize
   (fn [symbol]
     (let [resp (memoized-http-get (format "https://finance.yahoo.com/quote/%s/key-statistics" (str/upper-case symbol)))]
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
  (when (and (identity s) (string? s))
    (let [num  (->> (clojure.string/replace s #"," "") (re-find #"([0-9]*.[0-9]+|[0-9]+)") first e/as-float)
          unit (last s)]
      (when num
        (* (e/as-float num) (unit->value (str/lower-case unit) 1.0))))))

(defn- find-value-by-description [re htree]
  (->> htree
       (hs/select (hs/follow
                   (hs/has-child (hs/find-in-text re))
                   (hs/tag :td)))
       first
       :content
       first))

(defn market-cap [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^market cap")
       (->value)))

(defn enterprise-value [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value")
       (->value)))

(defn trailing-pe-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing p/e")
       (->value)))

(defn forward-pe-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward p/e")
       (->value)))

(defn peg-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^peg ratio")
       (->value)))

(defn price-to-sales-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^price/sales")
       (->value)))

(defn price-to-book-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value")
       (->value)))

(defn enterprise-value-to-revenue-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value/revenue")
       (->value)))

(defn enterprise-value-to-ebitda-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value/ebitda")
       (->value)))

(defn fiscal-year-ends [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^fiscal year ends"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(defn most-recent-quarter [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^most recent quarter"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(defn profit-margin [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^profit margin")
       (->value)))

(defn operating-margin [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^operating margin")
       (->value)))

(defn roa [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^return on assets")
       (->value)))

(defn roe [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^return on equity")
       (->value)))

(defn revenue [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^revenue$")
       (->value)))

(defn revenue-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^revenue per share")
       (->value)))

(defn quarterly-revenue-growth [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^quarterly revenue growth$")
       (->value)))

(defn gross-profit [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^gross profit$")
       (->value)))

(defn ebitda [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^ebitda$")
       (->value)))

(defn net-income-avi-to-common [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^net income avi to common$")
       (->value)))

(defn diluted-eps [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^diluted eps")
       (->value)))

(defn quarterly-earnings-growth [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^quarterly earnings growth$")
       (->value)))

(defn total-cash [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total cash$")
       (->value)))

(defn total-cash-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total cash per share$")
       (->value)))

(defn total-debt [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total debt$")
       (->value)))

(defn total-debt-to-equity [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total debt/equity$")
       (->value)))

(defn current-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^current ratio$")
       (->value)))

(defn book-value-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^book value per share$")
       (->value)))

(defn operating-cash-flow [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^operating cash flow$")
       (->value)))

(defn levered-free-cash-flow [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^levered free cash flow$")
       (->value)))

(defn beta [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^beta \(5y monthly\)$")
       (->value)))

(defn week-change-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52\-week change$")
       (->value)))

(defn s&p500-week-change-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^s&p500 52\-week change$")
       (->value)))

(defn week-high-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52 week high$")
       (->value)))

(defn week-low-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52 week low$")
       (->value)))

(defn day-ma-50 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^50\-day moving average$")
       (->value)))

(defn day-ma-200 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^200\-day moving average$")
       (->value)))

(defn avg-vol-3-month [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^avg vol \(3 month\)$")
       (->value)))

(defn avg-vol-10-day [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^avg vol \(10 day\)$")
       (->value)))

(defn shares-outstanding [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^shares outstanding$")
       (->value)))

(defn float [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^float$")
       (->value)))

(defn percent-held-by-insiders [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^\% held by insiders$")
       (->value)))

(defn percent-held-by-institutions [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^\% held by institutions$")
       (->value)))

(defn shares-short [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^shares short \(.+\)$")
       (->value)))

(defn short-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short ratio \(.+\)$")
       (->value)))

(defn short-percent-of-float [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short \% of float \(.+\)$")
       (->value)))

(defn short-percent-of-shares-outstanding [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short \% of shares outstanding \(.+\)$")
       (->value)))

(defn forward-annual-dividend-rate [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward annual dividend rate$")
       (->value)))

(defn forward-annual-dividend-yield [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward annual dividend yield$")
       (->value)))

(defn trailing-annual-dividend-rate [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing annual dividend rate$")
       (->value)))

(defn trailing-annual-dividend-yield [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing annual dividend yield$")
       (->value)))

(defn _5-year-average-dividend-yield  [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^5 year average dividend yield$")
       (->value)))

(defn payout-ratio  [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^payout ratio$")
       (->value)))

(defn dividend-date  [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^dividend date$"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(defn ex-dividend-date  [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^ex-dividend date$"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(defn valuation [symbol]
  (when (stock? symbol)
    {:market-cap                  (market-cap symbol)
     :enterprise-value            (enterprise-value symbol)
     :trailing-price-to-earnings  (trailing-pe-ratio symbol)
     :forward-price-to-earnings   (forward-pe-ratio symbol)
     :peg-ratio                   (peg-ratio symbol)
     :price-to-sales              (price-to-sales-ratio symbol)
     :price-to-book               (price-to-book-ratio symbol)
     :enterprise-value-to-revenue (enterprise-value-to-revenue-ratio symbol)
     :enterprise-value-to-ebitda  (enterprise-value-to-ebitda-ratio symbol)}))

(defn fiscal-year [symbol]
  (when (stock? symbol)
    {:fiscal-year-ends    (fiscal-year-ends symbol)
     :most-recent-quarter (most-recent-quarter symbol)}))

(defn profitability [symbol]
  (when (stock? symbol)
    {:profit-margin    (profit-margin symbol)
     :operating-margin (operating-margin symbol)}))

(defn management-effectivness [symbol]
  (when (stock? symbol)
    {:roa (roa symbol)
     :roe (roe symbol)}))

(defn income-statement [symbol]
  (when (stock? symbol)
    {:revenue                   (revenue symbol)
     :rps                       (revenue-per-share symbol)
     :quarterly-revenue-growth  (quarterly-revenue-growth symbol)
     :gross-profit              (gross-profit symbol)
     :ebitda                    (ebitda symbol)
     :net-income-avi-to-common  (net-income-avi-to-common symbol)
     :diluted-eps               (diluted-eps symbol)
     :quarterly-earnings-growth (quarterly-earnings-growth symbol)}))

(defn balance-sheet [symbol]
  (when (stock? symbol)
    {:total-cash           (total-cash symbol)
     :total-cash-per-share (total-cash-per-share symbol)
     :total-debt           (total-debt symbol)
     :total-debt-to-equity (total-debt-to-equity symbol)
     :current-ratio        (current-ratio symbol)
     :book-value-per-share (book-value-per-share symbol)}))

(defn cash-flow-statement [symbol]
  (when (stock? symbol)
    {:operating-cash-flow    (operating-cash-flow symbol)
     :levered-free-cash-flow (levered-free-cash-flow symbol)}))

(defn stock-price-history [symbol]
  (when (stock? symbol)
    {:beta                  (beta symbol)
     :week-change-52        (week-change-52 symbol)
     :sp500-52-week-change (s&p500-week-change-52 symbol)
     :week-high-52          (week-high-52 symbol)
     :week-low-52           (week-low-52 symbol)
     :day-ma-50             (day-ma-50 symbol)
     :day-ma-200            (day-ma-200 symbol)}))

(defn share-statistics [symbol]
  (when (stock? symbol)
    {:avg-vol-3-month                     (avg-vol-3-month symbol)
     :avg-vol-10-day                      (avg-vol-10-day symbol)
     :shares-outstanding                  (shares-outstanding symbol)
     :float                               (float symbol)
     :percent-held-by-insiders            (percent-held-by-insiders symbol)
     :percent-held-by-institutions        (percent-held-by-institutions symbol)
     :shares-short                        (shares-short symbol)
     :short-ratio                         (short-ratio symbol)
     :short-percent-of-float              (short-percent-of-float symbol)
     :short-percent-of-shares-outstanding (short-percent-of-shares-outstanding symbol)}))

(defn dividends [symbol]
  (when (stock? symbol)
    {:forward-annual-dividend-rate     (forward-annual-dividend-rate symbol)
     :forward-annual-dividend-yield    (forward-annual-dividend-yield symbol)
     :trailing-annual-dividend-rate    (trailing-annual-dividend-rate symbol)
     :trailing-annual-dividend-yield   (trailing-annual-dividend-yield symbol)
     :five-year-average-dividend-yield (_5-year-average-dividend-yield symbol)
     :payout-ratio                     (payout-ratio symbol)
     :dividend-date                    (dividend-date symbol)
     :ex-dividend-date                 (ex-dividend-date symbol)}))

(def ^:private get-profile-htree
  (e/memoize
   (fn [symbol]
     (let [resp (memoized-http-get (format "https://finance.yahoo.com/quote/%s/profile" (str/upper-case symbol)))]
       (when-not (seq (:trace-redirects resp))
         (-> resp
             :body
             hc/parse
             hc/as-hickory))))))

(defn company-name [symbol]
  (when (stock? symbol)
    (->> (get-profile-htree symbol)
         (hs/select (hs/descendant
                     (hs/attr :data-test #(= % "asset-profile"))
                     (hs/tag :h3)))
         first
         :content
         first
         str/lower-case)))

(defn company-address [symbol]
  (when (stock? symbol)
    (->> (get-profile-htree symbol)
         (hs/select (hs/descendant
                     (hs/attr :data-test #(= % "asset-profile"))
                     (hs/tag :div)
                     (hs/tag :p)))
         first
         :content
         (filter string?)
         ((fn [[street city country]]
            {:street  (some-> street (str/lower-case))
             :city    (some-> city (str/lower-case))
             :country (some-> country (str/lower-case))})))))

(defn company-info [symbol]
  (when (stock? symbol)
    (merge {:name (company-name symbol)}
           (company-address symbol))))

(defn company-profile [symbol]
  (->> (get-profile-htree (str/upper-case symbol))
       (hs/select (hs/descendant
                   (hs/attr :data-test #(= % "asset-profile"))
                   (hs/tag :div)
                   (hs/nth-of-type 2 :p)))
       first
       :content
       (mapv :content)
       ((fn [{[sector _] 4 [industry _] 10 [employees _] 16}]
          (let [employees]
            {:sector    (some-> sector (str/lower-case))
             :industry  (some-> industry (str/lower-case))
             :employees (some-> employees :content first (str/replace "," "") (e/as-?float))})))))

(defn symbol-name [symbol]
  (->> (memoized-http-get (str "https://finance.yahoo.com/quote/" (str/upper-case symbol)))
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
       ((fn [s] (-> s (str/split #"-") second (str/trim))))))
