#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/unsafe/ops
         syntax/parse/define
         uni-table
         uke/dataframe
         uke/index
         uke/series)

(define-syntax-rule (series/fc s val)
  (let ([v val])
    (unless (series? v)
      (error s "not a series ~s" v))
    v))


(define (show df #:nrows [nrows 10] #:widths [widths '(35)])
  (define table
    (let ()
      (define idx (dataframe-index df))
      (define series (dataframe-series df))
      (cons
       (for/list ([col (in-list series)])
         (series-name col))
       (for/list ([n (in-range nrows)]
                  [i (index-indices idx)])
         (for/list ([col (in-list series)])
           (series-ref (series/fc 'show col) i))))))
  (print-uni-table table
                   #:col-widths
                   (map list widths)
                   #:table-border
                   '(heavy solid)
                   #:row-align
                   '((wrap))
                   #:col-borders '((light))
                   #:row-borders '((light))))

(module* main #f
  (require racket/string
           threading
           uke/private/sawzall/create
           uke/private/sawzall/where)

  (define filename "capture_data.txt")

  (define (read-record inp)
    (define first-line (read-line inp))
    (cond
      [(eof-object? first-line) first-line]
      [else
        (define (read-cont)
          (define c (peek-char inp))
          (if (and (char? c) (char-whitespace? c))
              (string-append (read-line inp) (read-cont))
              ""))
        (string-append first-line (read-cont))]))

  (define data
    (call-with-input-file filename
      (lambda (inp)
        (for/dataframe (filename raw) ([a-line (in-port read-record inp)])
          (values filename a-line)))))


  (define (maybe-num v #:n/a [n/a values])
    (or (string->number v) (n/a v)))

  #;
  (~> (for/dataframe (n) ([i 20]) i)
      (where (n) (even? n))
      (create [o (n) (add1 n)])
      show)

  (define new-data
    (~> data
        #;
        (where (raw) (char-whitespace? (string-ref raw 0)))
        (create
         [netid  (raw) (substring raw 0 5)]
         [state  (raw) (substring raw 5 10)]
         [recv-q (raw) (~> (substring raw 10 22)
                           string-trim
                           maybe-num)]
         [send-q (raw) (~> (substring raw 22 30)
                           string-trim
                           maybe-num)])))
  (show new-data)
  )
