(ns ribelo.yf
  (:refer-clojure :exclude [float])
  (:require
   [taoensso.encore :as e]
   [clj-http.client :as http]
   [java-time :as jt]
   [jsonista.core :as json]
   [hickory.core :as hc]
   [hickory.select :as hs]
   [cuerdas.core :as str]
   [meander.epsilon :as m]))

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
          resp    (http/get (format url (str/upper (name symbol))))
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
                              (str/upper (name symbol))
                              start-time
                              end-time
                              interval
                              event
                              crumb)
                      {:cookies cookies})
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
                            :volume (e/as-?float volume)}))
                    (filter (fn [m] (every? identity (vals m))))))))
     (throw (ex-info "empty response" {:symbol symbol})))))

(comment
  (get-data "msft"))

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
  (when (and (identity s) (string? s))
    (let [num  (->> (clojure.string/replace s #"," "") (re-find #"([0-9]*.[0-9]+|[0-9]+)") first e/as-?float)
          unit (last s)]
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

(defn market-cap [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^market cap")
       (->value)))

(comment
  (market-cap "msft"))

(defn enterprise-value [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value")
       (->value)))

(comment
  (enterprise-value "msft"))

(defn trailing-pe-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing p/e")
       (->value)))

(comment
  (trailing-pe-ratio "msft"))

(defn forward-pe-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward p/e")
       (->value)))

(comment
  (forward-pe-ratio "msft"))

(defn peg-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^peg ratio")
       (->value)))

(comment
  (peg-ratio "msft")
  ;; => 2.28
  )

(defn price-to-sales-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^price/sales")
       (->value)))

(comment
  (price-to-sales-ratio "msft")
  ;; => 11.79
  )

(defn price-to-book-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value")
       (->value)))

(comment
  (price-to-book-ratio "msft")
  ;; => 1.69
  )

(defn enterprise-value-to-revenue-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value/revenue")
       (->value)))

(comment
  (enterprise-value-to-revenue-ratio "msft")
  ;; => 11.03
  )

(defn enterprise-value-to-ebitda-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^enterprise value/ebitda")
       (->value)))

(comment
  (enterprise-value-to-ebitda-ratio "msft")
  ;; => 23.58
  )

(defn fiscal-year-ends [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^fiscal year ends"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(comment
  (jt/local-date "yyyy-MM-dd" "2020-01-01")
  (jt/local-date "dd LLL yyyy" "16 May 2018")
  (fiscal-year-ends "msft")
  ;; => #object[java.time.LocalDate 0x109b8b40 "2020-06-29"]
  )

(defn most-recent-quarter [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^most recent quarter"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(comment
  (most-recent-quarter "msft")
  ;; => #object[java.time.LocalDate 0x34386d21 "2020-12-30"]
  )

(defn profit-margin [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^profit margin")
       (->value)))

(comment
  (profit-margin "msft")
  ;; => 33.47
  )

(defn operating-margin [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^operating margin")
       (->value)))

(comment
  (operating-margin "msft")
  ;; => 39.24
  )

(defn roa [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^return on assets")
       (->value)))

(comment
  (roa "msft")
  ;; => 12.81
  )

(defn roe [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^return on equity")
       (->value)))

(comment
  (roe "msft")
  ;; => 42.7
  )

(defn revenue [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^revenue$")
       (->value)))

(comment
  (revenue "msft")
  ;; => 153.28
  )

(defn revenue-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^revenue per share")
       (->value)))

(comment
  (revenue-per-share "msft")
  ;; => 20.23
  )

(defn quarterly-revenue-growth [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^quarterly revenue growth$")
       (->value)))

(comment
  (quarterly-revenue-growth "msft")
  ;; => 16.7
  )

(defn gross-profit [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^gross profit$")
       (->value)))

(comment
  (gross-profit "msft")
  ;; => 96.94
  )

(defn ebitda [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^ebitda$")
       (->value)))

(comment
  (ebitda "msft")
  ;; => 71.69
  )

(defn net-income-avi-to-common [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^net income avi to common$")
       (->value)))

(comment
  (net-income-avi-to-common "msft")
  ;; => 51.31
  )

(defn diluted-eps [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^diluted eps")
       (->value)))

(comment
  (diluted-eps "msft")
  ;; => 6.2
  )

(defn quarterly-earnings-growth [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^quarterly earnings growth$")
       (->value)))

(comment
  (quarterly-earnings-growth "msft")
  ;; => 32.7
  )

(defn total-cash [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total cash$")
       (->value)))

(comment
  (total-cash "msft")
  ;; => 131.97
  )

(defn total-cash-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total cash per share$")
       (->value)))

(comment
  (total-cash-per-share "msft")
  ;; => 17.49
  )

(defn total-debt [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total debt$")
       (->value)))

(comment
  (total-debt "msft")
  ;; => 69.4
  )

(defn total-debt-to-equity [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^total debt/equity$")
       (->value)))

(comment
  (total-debt-to-equity "msft")
  ;; => 53.29
  )

(defn current-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^current ratio$")
       (->value)))

(comment
  (current-ratio "msft")
  ;; => 2.58
  )

(defn book-value-per-share [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^book value per share$")
       (->value)))

(comment
  (book-value-per-share "msft")
  ;; => 16.31
  )

(defn operating-cash-flow [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^operating cash flow$")
       (->value)))

(comment
  (operating-cash-flow "msft")
  ;; => 68.03
  )

(defn levered-free-cash-flow [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^levered free cash flow$")
       (->value)))

(comment
  (levered-free-cash-flow "msft")
  ;; => 36.83
  )

(defn beta [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^beta \(5y monthly\)$")
       (->value)))

(comment
  (beta "msft")
  ;; => nil
  )

(defn week-change-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52\-week change$")
       (->value)))

(comment
  (week-change-52 "msft")
  ;; => 38.26
  )

(defn s&p500-week-change-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^s&p500 52\-week change$")
       (->value)))

(comment
  (s&p500-week-change-52 "msft")
  ;; => 17.6
  )

(defn week-high-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52 week high$")
       (->value)))

(comment
  (week-high-52 "msft")
  ;; => 240.18
  )

(defn week-low-52 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^52 week low$")
       (->value)))

(comment
  (week-low-52 "msft")
  ;; => 132.52
  )

(defn day-ma-50 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^50\-day moving average$")
       (->value)))

(comment
  (day-ma-50 "msft")
  ;; => 219.23
  )

(defn day-ma-200 [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^200\-day moving average$")
       (->value)))

(comment
  (day-ma-200 "msft")
  ;; => 213.48
  )

(defn avg-vol-3-month [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^avg vol \(3 month\)$")
       (->value)))

(comment
  (avg-vol-3-month "msft")
  ;; => 29.2
  )

(defn avg-vol-10-day [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^avg vol \(10 day\)$")
       (->value)))

(comment
  (avg-vol-10-day "msft")
  ;; => 35.17
  )

(defn shares-outstanding [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^shares outstanding$")
       (->value)))

(comment
  (shares-outstanding "msft")
  ;; => 7.56
  )

(defn float [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^float$")
       (->value)))

(comment
  (float "msft")
  ;; => 7.44
  )

(defn percent-held-by-insiders [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^\% held by insiders$")
       (->value)))

(comment
  (percent-held-by-insiders "msft")
  ;; => 0.06
  )

(defn percent-held-by-institutions [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^\% held by institutions$")
       (->value)))

(comment
  (percent-held-by-institutions "msft")
  ;; => 71.84
  )

(defn shares-short [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^shares short \(.+\)$")
       (->value)))

(comment
  (shares-short "msft")
  ;; => 39.2
  )

(defn short-ratio [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short ratio \(.+\)$")
       (->value)))

(comment
  (short-ratio "msft")
  ;; => 1.44
  )

(defn short-percent-of-float [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short \% of float \(.+\)$")
       (->value)))

(comment
  (short-percent-of-float "msft")
  ;; => 0.52
  )

(defn short-percent-of-shares-outstanding [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^short \% of shares outstanding \(.+\)$")
       (->value)))

(comment
  (short-percent-of-shares-outstanding "msft")
  ;; => 0.52
  )

(defn forward-annual-dividend-rate [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward annual dividend rate$")
       (->value)))

(comment
  (forward-annual-dividend-rate "msft")
  ;; => 2.24
  )

(defn forward-annual-dividend-yield [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^forward annual dividend yield$")
       (->value)))

(comment
  (forward-annual-dividend-yield "msft")
  ;; => 0.96
  )

(defn trailing-annual-dividend-rate [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing annual dividend rate$")
       (->value)))

(comment
  (trailing-annual-dividend-rate "msft")
  ;; => 2.09
  )

(defn trailing-annual-dividend-yield [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^trailing annual dividend yield$")
       (->value)))

(comment
  (trailing-annual-dividend-yield "msft")
  ;; => 0.9
  )

(defn _5-year-average-dividend-yield  [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^5 year average dividend yield$")
       (->value)))

(comment
  (_5-year-average-dividend-yield "msft")
  ;; => 1.74
  )

(defn payout-ratio  [symbol]
  (->> (get-key-stats-htree symbol)
       (find-value-by-description #"(?i)^payout ratio$")
       (->value)))

(comment
  (payout-ratio "msft")
  ;; => 32.9
  )

(defn dividend-date  [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^dividend date$"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(comment
  (dividend-date "msft")
  ;; => #object[java.time.LocalDate 0x14e97ac6 "2021-03-10"]
  )

(defn ex-dividend-date  [symbol]
  (when-let [s (->> (get-key-stats-htree symbol)
                    (find-value-by-description #"(?i)^ex-dividend date$"))]
    (when (string? s) (jt/local-date "LLL dd, yyyy" s))))

(comment
  (ex-dividend-date "msft")
  ;; => #object[java.time.LocalDate 0x4a8632d8 "2021-02-16"]
  )

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

(comment
  (valuation "msft")
  ;; =>
  ;; {:forward-price-to-earnings 31.93,
  ;;  :market-cap 1.81,
  ;;  :price-to-book 1.69,
  ;;  :enterprise-value-to-revenue 11.03,
  ;;  :price-to-sales 11.78,
  ;;  :enterprise-value 1.69,
  ;;  :enterprise-value-to-ebitda 23.58,
  ;;  :trailing-price-to-earnings 38.53,
  ;;  :peg-ratio 2.28}
  )

(defn fiscal-year [symbol]
  (when (stock? symbol)
    {:fiscal-year-ends    (fiscal-year-ends symbol)
     :most-recent-quarter (most-recent-quarter symbol)}))

(comment
  (fiscal-year "msft")
  ;; =>
  ;; {:fiscal-year-ends #object[java.time.LocalDate
  ;;                            0x3dfb1e92
  ;;                            "2020-06-29"]
  ;;  , :most-recent-quarter
  ;;  #object[java.time.LocalDate
  ;;          0x1640f2f5
  ;;          "2020-12-30"]}
  )

(defn profitability [symbol]
  (when (stock? symbol)
    {:profit-margin    (profit-margin symbol)
     :operating-margin (operating-margin symbol)}))

(comment
  (profitability "msft")
  ;; => {:profit-margin 33.47, :operating-margin 39.24}
  )

(defn management-effectivness [symbol]
  (when (stock? symbol)
    {:roa (roa symbol)
     :roe (roe symbol)}))

(comment
  (management-effectivness "msft")
  ;; => {:roa 12.81, :roe 42.7}
  )

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

(comment
  (income-statement "msft")
  ;; =>
  ;; {:revenue 153.28,
  ;;  :rps 20.23,
  ;;  :quarterly-revenue-growth 16.7,
  ;;  :gross-profit 96.94,
  ;;  :ebitda 71.69,
  ;;  :net-income-avi-to-common 51.31,
  ;;  :diluted-eps 6.2,
  ;;  :quarterly-earnings-growth 32.7}
  )

(defn balance-sheet [symbol]
  (when (stock? symbol)
    {:total-cash           (total-cash symbol)
     :total-cash-per-share (total-cash-per-share symbol)
     :total-debt           (total-debt symbol)
     :total-debt-to-equity (total-debt-to-equity symbol)
     :current-ratio        (current-ratio symbol)
     :book-value-per-share (book-value-per-share symbol)}))

(comment
  (balance-sheet "msft")
  ;; =>
  ;; {:total-cash 131.97,
  ;;  :total-cash-per-share 17.49,
  ;;  :total-debt 69.4,
  ;;  :total-debt-to-equity 53.29,
  ;;  :current-ratio 2.58,
  ;;  :book-value-per-share 16.31}
  )

(defn cash-flow-statement [symbol]
  (when (stock? symbol)
    {:operating-cash-flow    (operating-cash-flow symbol)
     :levered-free-cash-flow (levered-free-cash-flow symbol)}))

(comment
  (cash-flow-statement "msft")
  ;; => {:operating-cash-flow 68.03, :levered-free-cash-flow 36.83}
  )

(defn stock-price-history [symbol]
  (when (stock? symbol)
    {:beta                 (beta symbol)
     :week-change-52       (week-change-52 symbol)
     :sp500-52-week-change (s&p500-week-change-52 symbol)
     :week-high-52         (week-high-52 symbol)
     :week-low-52          (week-low-52 symbol)
     :day-ma-50            (day-ma-50 symbol)
     :day-ma-200           (day-ma-200 symbol)}))

(comment
  (stock-price-history "msft")
  ;; =>
  ;; {:beta nil,
  ;;  :week-change-52 38.26,
  ;;  :sp500-52-week-change 17.6,
  ;;  :week-high-52 240.18,
  ;;  :week-low-52 132.52,
  ;;  :day-ma-50 219.23,
  ;;  :day-ma-200 213.48}
  )

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

(comment
  (share-statistics "msft")
  ;; =>
  ;; {:shares-outstanding 7.56,
  ;;  :short-ratio 1.44,
  ;;  :float 7.44,
  ;;  :short-percent-of-shares-outstanding 0.52,
  ;;  :avg-vol-3-month 29.2,
  ;;  :percent-held-by-insiders 0.06,
  ;;  :shares-short 39.2,
  ;;  :short-percent-of-float 0.52,
  ;;  :avg-vol-10-day 35.17,
  ;;  :percent-held-by-institutions 71.84}
  )

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

(comment
  (dividends "msft")
  ;; =>
  ;; {:forward-annual-dividend-rate 2.24,
  ;;  :forward-annual-dividend-yield 0.96,
  ;;  :trailing-annual-dividend-rate 2.09,
  ;;  :trailing-annual-dividend-yield 0.9,
  ;;  :five-year-average-dividend-yield 1.74,
  ;;  :payout-ratio 32.9,
  ;;  :dividend-date #object[java.time.LocalDate
  ;;                         0xcd1f7f1
  ;;                         "2021-03-10"]
  ;;  , :ex-dividend-date
  ;;  #object[java.time.LocalDate
  ;;          0x75b24a5
  ;;          "2021-02-16"]}
  )

(def ^:private get-profile-htree
  (e/memoize
      (fn [symbol]
        (let [resp (memoized-http-get (format "https://finance.yahoo.com/quote/%s/profile" (str/upper symbol)))]
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
         str/lower)))

(comment
  (company-name "msft")
  ;; => microsoft corporation
  )

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
            {:street  (some-> street (str/lower))
             :city    (some-> city (str/lower))
             :country (some-> country (str/lower))})))))

(comment
  (company-address "msft")
  ;; =>
  ;; {:street "one microsoft way"
  ;;  , :city
  ;;  "redmond, wa 98052-6399" ,
  ;;  :country "united states"}
  )

(defn company-info [symbol]
  (when (stock? symbol)
    (merge {:name (company-name symbol)}
           (company-address symbol))))

(comment
  (company-info "msft")
  ;; =>
  ;; {:name "microsoft corporation"
  ;;  , :street
  ;;  "one microsoft way" ,
  ;;  :city "redmond, wa 98052-6399"
  ;;  , :country
  ;;  "united states"}
  )

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
          {:sector    (some-> sector (str/lower))
           :industry  (some-> industry (str/lower))
           :employees (some-> employees :content first (str/replace "," "") (e/as-?float))}))))

(comment
  (company-profile "msft")
  ;; =>
  ;; {:sector "technology", :industry "software—infrastructure", :employees 163000.0}
  )

(defn symbol-name [symbol]
  (->> (memoized-http-get (str "https://finance.yahoo.com/quote/" (str/upper symbol)))
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

(comment
  (symbol-name "msft")
  ;; => microsoft corporation
  )

(->> (http/get "https://finance.yahoo.com/quote/MSFT/options")
     :body
     (hc/parse)
     (hc/as-hickory)
     (hs/select (hs/tag :select)))

(comment
  (http/get "msft")
  ;; => error: java.net.MalformedURLException: no protocol: msft
  )

(defn- parse-options [data]
  (m/match data
    {:option-chain
     {:result
      [(m/some {:underlying-symbol ?symbol
                :expiration-dates  ?dates
                :options           [{:calls [{:volume !calls-volume
                                              :strike !calls-strike} ...]
                                     :puts  [{:volume !puts-volume
                                              :strike !puts-strike} ...]} & _]})
       & _]}}
    {:expiration-dates ?dates
     :calls-volume     (->> !calls-volume (mapv #(or % 0)))
     :calls-strike     !calls-strike
     :puts-volume      (->> !puts-volume (mapv #(or % 0)))
     :puts-strike      !puts-strike}))

(defn- -options
  ([symbol date]
   (m/match date
     :all
     (let [base  (-options symbol)
           dates (-> base :expiration-dates (rest))
           {:keys [calls-volume calls-strike
                   puts-volume puts-strike]}
           (reduce
             (fn [acc date]
               (let [{:keys [calls-volume calls-strike
                             puts-volume puts-strike]} (-options symbol date)]
                 (-> acc
                     (update :calls-volume into calls-volume)
                     (update :calls-strike into calls-strike)
                     (update :puts-volume into puts-volume)
                     (update :puts-strike into puts-strike))))
             base dates)]
       {:calls-delta (/ (reduce + (mapv * calls-volume calls-strike))
                        (reduce + calls-volume))
        :puts-delta  (/ (reduce + (mapv * puts-volume puts-strike))
                        (reduce + puts-volume))})
     _
     (-> (http/get (format "https://query1.finance.yahoo.com/v7/finance/options/%s?date=%s" (name symbol) date))
         :body
         (json/read-value json/keyword-keys-object-mapper)
         (parse-options))))
  ([symbol]
   (-> (http/get (format "https://query1.finance.yahoo.com/v7/finance/options/%s" (name symbol)))
       :body
       (json/read-value json/keyword-keys-object-mapper))))

(comment
  (-options "msft"))

(defn options-expiration-dates [symbol]
  (-> (-options symbol)
      (parse-options)
      :expiration-dates))
(-options "msft")
;; =>
;; {:expiration-dates [1603411200 1604016000 1604620800 1605225600 1605830400 1606435200 1608249600 1610668800 1613692800 1616112000 1618531200 1623974400 1626393600 1631836800 1642723200 1647561600 1655424000 1663286400 1674172800], :calls-volume 208191, :puts-volume 93067, :delta-volume 115124}
