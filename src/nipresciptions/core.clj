(ns nipresciptions.core
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as s-de]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:gen-class))


(def fields [:practice :year :month :vtm_nm :vmp_nm :amp_nm :presentation :strength :total-items :total-quantity :gross-cost :actual-cost :bnfcode :bnfchapter :bnfsection :bnfparagraph :bnfsub-paragraph :noname1 :noname2])

(def practice-fields [:pracno :partnershipno :practicename :address1 :address2 :address3 :postcode :telno :lcg])

(defn load-practices [sc filepath] 
  (->> (spark/text-file sc filepath)
       (spark/map #(->> (csv/read-csv %) first))
       (spark/filter #(not= (first %) "PracNo"))
       (spark/map #(zipmap practice-fields %))
       (spark/map-to-pair (fn [rec] 
                            (let [practice-id (:pracno rec)]
                              (spark/tuple practice-id rec))))))

(defn load-prescription-data [sc filepath] 
  (->> (spark/text-file sc filepath)
       (spark/map #(->> (csv/read-csv %) first))
       (spark/filter #(not= (first %) "PRACTICE"))
       (spark/map #(zipmap fields %))
       (spark/map-to-pair (fn [rec]
                            (let [practicekey (:practice rec)]
                              (spark/tuple practicekey rec))))
       (spark/group-by-key)))

(defn join-practice-prescriptions [practices prescriptions]
  (spark/join practices prescriptions))

(defn def-practice-prescription-freq [prescriptiondata]
  (->> prescriptiondata
       (spark/map-to-pair (s-de/key-val-val-fn (fn [k v pr] 
                                               (let [freqmap (map (fn [rec] (:vmp_nm rec)) pr)]
                                                 (spark/tuple (str (:practicename v) " " (:postcode v)) (apply list (take 10 (reverse (sort-by val (frequencies freqmap)))))))))))) 

(defn process-data [sc filepath practicefile outputpath] 
  (let [prescription-rdd (load-prescription-data sc filepath)
        practice-rdd (load-practices sc practicefile)]
    (->> (join-practice-prescriptions practice-rdd prescription-rdd)
         (def-practice-prescription-freq)
         (spark/coalesce 1)
         (spark/save-as-text-file outputpath))))



(comment 
  (def c (-> (conf/spark-conf)
             (conf/master "local[3]")
             (conf/app-name "niprescriptions-sparkjob")))
  (def sc (spark/spark-context c))



)

