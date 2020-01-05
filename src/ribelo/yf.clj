(ns ribelo.yf
  (:require
   [taoensso.encore :as e]
   [clj-http.client :as http]
   [clojure.string :as str]
   [java-time :as jt]))

(defn str-time->epoch [s]
  (quot (jt/to-millis-from-epoch (jt/java-date (jt/local-date s) (jt/zone-offset 0))) 1000))

(defn get-cookie&crumb [symbol]
  (let [url     "https://finance.yahoo.com/quote/%s/history"
        resp    (http/get (format url (str/upper-case (name symbol))))
        cookies (:cookies resp)
        body    (:body resp)
        crumb   (second (re-find #"\"CrumbStore\":\{\"crumb\":\"(.{11})\"\}" body))]
    (when crumb
      [cookies crumb])))

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
                          {:symbol (str/lower-case symbol)
                           :date   (jt/local-date date)
                           :open   (e/as-?float open)
                           :high   (e/as-?float high)
                           :low    (e/as-?float low)
                           :close  (e/as-?float close)
                           :volume (e/as-?float volume)}))
                   (filter (fn [m] (every? identity (vals m))))))))
     (throw (ex-info "empty response" {:symbol symbol})))))
