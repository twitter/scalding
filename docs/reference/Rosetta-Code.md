# Rosetta Code

A collection of MapReduce tasks translated (from Pig, Hive, MapReduce streaming, etc.) into Scalding.
For fully runnable code, see the repository [here](https://github.com/echen/rosetta-scone).

## Word Count

### Hadoop Streaming (Ruby)

```ruby
# Emit (word, count) pairs.
def mapper
  STDIN.each_line do |line|
    line.split.each do |word|
      puts [word, 1].join("\t")
    end
  end
end

# Aggregate all (word, count) pairs for a particular word.
#
# In Hadoop Streaming (unlike standard Hadoop), the reducer receives
# rows from the mapper *one at a time*, though the rows are guaranteed
# to be sorted by key (and every row associated to a particular key
# will be sent to the same reducer).
def reducer
  curr_word = nil
  curr_count = 0
  STDIN.each_line do |line|
    word, count = line.strip.split("\t")
    if word != curr_word
      puts [curr_word, curr_count].join("\t")
      curr_word = word
      curr_count = 0
    end
    curr_count += count.to_i
  end

  puts [curr_word, curr_count].join("\t") unless curr_word.nil?
end
```

### Hive
```python
# tokenizer.py
import sys

for line in sys.stdin:
  for word in line.split():
    print word
```

### SQL
```sql
CREATE TABLE tweets (text STRING);
LOAD DATA LOCAL INPATH 'tweets.tsv' OVERWRITE INTO TABLE tweets;

SELECT word, COUNT(*) AS count
FROM (
  SELECT TRANSFORM(text) USING 'python tokenizer.py' AS word
  FROM tweets
) t
GROUP BY word;
```

### Pig
```pig
tweets = LOAD 'tweets.tsv' AS (text:chararray);
words = FOREACH tweets GENERATE FLATTEN(TOKENIZE(text)) AS word;
word_groups = GROUP words BY word;
word_counts = FOREACH word_groups GENERATE group AS word, COUNT(words) AS count;

STORE word_counts INTO 'word_counts.tsv';
```

### Cascalog 2.0

```clojure
(cascalog.repl/bootstrap)

(?<- (hfs-textline "word_counts.tsv") [?word ?count]
     ((hfs-textline "tweets.tsv") ?text)
     ((mapcatfn [text] (.split text "\\s+")) ?text :> ?word)
     (c/count ?count)))
```

### Scalding
```scala
import com.twitter.scalding._
import com.twitter.scalding.source.TypedText

class ScaldingTestJob(args: Args) extends Job(args) {
  TypedText.tsv[String]("tweets.tsv")
    .flatMap(_.split("\\s+")
    .groupBy(_.size)
    .write(TypedText.tsv[String]("word_counts.tsv"))
}
```

## Distributed Grep

### Hadoop Streaming (Ruby)
```ruby
PATTERN = /.*hello.*/

# Emit words that match the pattern.
def mapper
  STDIN.each_line do |line|
    puts line if line =~ PATTERN
  end
end

# Identity reducer.
def reducer
  STDIN.each_line do |line|
    puts line
  end
end
```

### Pig
```pig
%declare PATTERN '.*hello.*';

tweets = LOAD 'tweets.tsv' AS (text:chararray);
results = FILTER tweets BY (text MATCHES '$PATTERN');
```

### Cascalog
```clojure
(def pattern #".*hello.*")

(deffilterop matches-pattern? [text pattern]
  (re-matches pattern text))

(defn distributed-grep [input pattern]
  (<- [?textline]
      (input ?textline)
      (matches-pattern? ?textline pattern)))

(?- (stdout) (distributed-grep (hfs-textline "tweets.tsv") pattern))
```

### Scalding
```scala
import com.twitter.scalding.source.TypedText

val Pattern = ".*hello.*".r

TypedText.tsv[String]("tweets.tsv").filter { _.matches(Pattern) }
```

## Inverted Index

### Hadoop Streaming (Ruby)

```ruby
# Emit (word, tweet_id) pairs.
def mapper
  STDIN.each_line do |line|
    tweet_id, text = line.strip.split("\t")
    text.split.each do |word|
      puts [word, tweet_id].join("\t")
    end
  end
end

# Aggregate all (word, tweet_id) pairs for a particular word.
#
# In Hadoop Streaming (unlike standard Hadoop), the reducer receives
# rows from the mapper *one at a time*, though the rows are guaranteed
# to be sorted by key (and every row associated to a particular key
# will be sent to the same reducer).
def reducer
  curr_word = nil
  curr_inv_index = []
  STDIN.each_line do |line|
    word, tweet_id = line.strip.split("\t")
    if word != curr_word
      # New key.
      puts [curr_word, curr_inv_index.join(",")].join("\t")
      curr_word = word
      curr_inv_index = []
    end
    curr_inv_index << tweet_id
  end

  unless curr_word.nil?
    puts [curr_word, curr_inv_index.join(", ")].join("\t")
  end
end
```

### Pig
```pig
tweets = LOAD 'tweets.tsv' AS (tweet_id:int, text:chararray);

words = FOREACH tweets GENERATE tweet_id, FLATTEN(TOKENIZE(text)) AS word;
word_groups = GROUP words BY word;
inverted_index = FOREACH word_groups GENERATE group AS word, words.tweet_id;
```

### Cascalog
```clojure
;; define the data
(def index [
  [0 "Hello World"]
  [101 "The quick brown fox jumps over the lazy dog"]
  [42 "Answer to the Ultimate Question of Life, the Universe, and Everything"]
])

;; the tokenize function
(defmapcatop tokenize [text]
  (seq (.split text "\\s+")))

;; ensure inverted index is distinct per word
(defbufferop distinct-vals [tuples]
  (list (set (map first tuples))))

;; run the query on data
(?<- (stdout) [?word ?ids]
        (index ?id ?text)
        (tokenize ?text :> ?word)
        (distinct-vals ?id :> ?ids))
```

### Scalding

```scala
import com.twitter.scalding.source.TypedText

val invertedIndex =
  TypedText.tsv[Int,String]("tweets.tsv")
  .flatMap { case (tweetId, text) => text.split("\\s+").map((_, tweetId)) }
  .group
```
