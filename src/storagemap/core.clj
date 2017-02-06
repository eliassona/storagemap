(ns storagemap.core
  (:import [clojure.lang MapEntry]
           [java.util AbstractMap$SimpleEntry]))

(defprotocol IStorage
 (s-write! [this k v]) 
 (s-read [this k])
 (s-delete! [this k])
 (s-keys [this query])
 )

(defprotocol ISerializer
  (serialize [this data])
  (deserialize [this data]))

(defprotocol IPersistence
  (store! [this]))

(defn key-of 
  ([prefix k]
    (str prefix k))
  ([prefixed-key]
   (if-let [k (second (clojure.string/split prefixed-key #":"))]
     k
     prefixed-key))) 
   
(defn entries-of 
  ([storage prefix serializer assocMap]
  (loop [entries []
         assoc-map assocMap
         ks (s-keys storage (key-of prefix "*"))]
    (if (not (empty? ks))
      (let [pk (first ks)
            k (key-of pk)]
        (if (contains? assocMap k)
          (recur (conj entries (AbstractMap$SimpleEntry. k (get assocMap k))) (dissoc assoc-map k) (rest ks))
          (recur (conj entries (AbstractMap$SimpleEntry. k (.deserialize serializer (s-read storage pk)))) assoc-map (rest ks))))
      [entries assoc-map])))
  ([storage prefix serializer assocMap withouts]
    (let [[entries assoc-map] (entries-of storage prefix serializer assocMap)]
      (concat 
        (map #(AbstractMap$SimpleEntry. (key %) (val %)) assoc-map)
        (filter
          (fn [e]
            (not (contains? withouts (.getKey e))))
          entries)))))



(deftype StoragePersistentMap [storage prefix serializer withouts assocMap]
  clojure.lang.IFn
  (invoke [this k]
    (deserialize serializer (s-read storage (key-of prefix k))))
  
  (invoke [this k not_found]
    (if-let [v (.invoke this k)]
      v
      not_found))
  
  clojure.lang.IPersistentMap
  (assoc [this k v]
    (StoragePersistentMap. storage prefix serializer (disj withouts k) (assoc assocMap k v)))
    
  (assocEx [this k v]
    (if (.containsKey this k)
      (throw (IllegalStateException. "Key already present"))
      (.assoc this k v)))
  
  (without [this k]
    (if (.containsKey this k)
      (StoragePersistentMap. storage prefix serializer (conj withouts k) (dissoc assocMap k))
      this))
  
  java.lang.Iterable
  (iterator [this] (.iterator (entries-of storage prefix serializer assocMap withouts)))
  
  clojure.lang.Associative
  (containsKey [this k]
    (cond 
      (contains? assocMap k)
      true
      (contains? withouts k)
      false
      :else
      (= (count (s-keys storage (key-of prefix k))) 1)))
  
  (entryAt [this k] (MapEntry. k (s-read storage (key-of prefix k))))
  
  clojure.lang.IPersistentCollection
  
  (count [_] 
    (let [ks (into #{} (map key-of (s-keys storage (key-of prefix "*"))))]
      (- 
        (+
          (count (filter #(not (contains? ks %)) (keys assocMap)))
          (count ks))
        (count withouts))))
               
  
  (cons [this [k v]] this) ;TODO
  
  (empty [this] 
    (StoragePersistentMap. storage prefix serializer (into #{} (keys this)) {}))
  
  (equiv [this o]
    (if (= (.count this) (count o))
      (every? (fn [e] (= (val e) (get o (key e)))) this)
      false))
  
  clojure.lang.Seqable
  (seq [this] (.seq (entries-of storage prefix serializer assocMap withouts)))
  
  
  clojure.lang.ILookup
  (valAt [this k]
    (cond 
      (contains? assocMap k)
      (get assocMap k)
      (contains? withouts k)
      nil
      :else
      (.deserialize serializer (s-read storage (key-of prefix k)))))
  
  (valAt [this k not_found] (if-let [v (.valAt this k)] v not_found))

  clojure.lang.MapEquivalence
  
  java.util.Map
  (size [this] (.count this))
  (isEmpty [this] (<= (.count this)))
  (get [this k] (.valAt this k))
  
  IPersistence
  (store! [this]
    (doseq [e assocMap]
      (s-write! storage (key-of prefix (key e)) (.serialize serializer (val e))))
    (doseq [k withouts]
      (s-delete! storage (key-of prefix k)))))


(defn storage-map [storage prefix serializer]
  (StoragePersistentMap. storage (if (empty? prefix) prefix (str prefix ":")) serializer #{} {}))                   
                   
