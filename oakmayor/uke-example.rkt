#lang racket/base

(require explorer
         racket/pretty
         racket/sequence
         racket/string
         threading
         uke/dataframe
         uke/index
         uke/series
         uke/store
         uke/private/sawzall/create
         uke/private/sawzall/show
         uke/private/sawzall/slice
         uke/private/sawzall/where)

(define (file->dataframe filename [read-row read-line])
  (call-with-input-file filename
    (lambda (inp)
      (for/dataframe (filename raw) ([a-line (in-port read-row inp)])
        (values filename a-line)))))

(define (inspect df)
  (pretty-print (dataframe-inspect df))
  df)

(define candidates
  (~> (file->dataframe "master_lookup.txt")
      (slice (not filename))
      (create [record-type (raw) (~> raw (substring 0 10) string-trim)])
      (where (record-type) (string=? record-type "Candidate"))
      (create [candidate-id   (raw) (substring raw 11 17)]
              [candidate-name (raw) (string-trim (substring raw 17 67))])
      (slice (not (or raw record-type)))))

(define votes
  (~> (file->dataframe "oak_mayor_20181117.txt")
      (slice (not filename))
      (create [rank         (raw) (string->number (substring raw 33 36))]
              [voter        (raw) (substring raw 7 16)]
              [candidate-id (raw) (substring raw 36 43)])
      (slice (not raw))))

(define (series-build-index a-series)
  (define an-index (series-index a-series))
  ;; XXX handle multiple ptr index
  (define store-indices
    (sequence-map (λ (i) (index-lookup an-index i))
                  (index-indices an-index)))
  (define a-store (series-store a-series))
  (define (reorder-index-values a-hash)
    (for/hash ([(k v) (in-hash a-hash)])
      (values k (reverse v))))
  (for*/fold ([new-idx (hash)] #:result (reorder-index-values new-idx))
             ([i store-indices]
              [v (in-value (store-ref a-store i))])
    (hash-update new-idx v (λ (ptrs) (cons i ptrs)) null)))

(define idx-a
  (series-build-index (dataframe-series-ref candidates 'candidate-id)))

(define idx-b
  (series-build-index (dataframe-series-ref votes 'candidate-id)))

#;
(~> votes
    (where (candidate-id) (string=? candidate-id "0000000"))
    (show #:nrows 1000))

