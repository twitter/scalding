(ns cascalog.tap
  (:require [cascalog.workflow :as w])
  (:import [cascading.tuple Fields]))

;; source can be a cascalog-tap, subquery, or cascading tap sink can
;; be a cascading tap, a sink function, or a cascalog-tap

(defstruct cascalog-tap :type :source :sink)

(defn mk-cascalog-tap
  "Defines a cascalog tap which can be used to add additional
  abstraction over cascading taps.
  
   'source' can be a cascading tap, subquery, or a cascalog tap.
   'sink' can be a cascading tap, sink function, or a cascalog tap."
  [source sink]
  (struct-map cascalog-tap :type :cascalog-tap :source source :sink sink))

(defn- patternize
  "If `pattern` is truthy, returns the supplied parent `Hfs` or `Lfs`
  tap wrapped that responds as a `TemplateTap` when used as a sink,
  and a `GlobHfs` tap when used as a source. Otherwise, acts as
  identity."
  [scheme type path-or-file sinkmode sink-template source-pattern templatefields]
  (let [tap-maker ({:hfs w/hfs :lfs w/lfs} type)
        parent (tap-maker scheme path-or-file sinkmode)
        source (if source-pattern
                 (w/glob-hfs scheme path-or-file source-pattern)
                 parent)
        sink (if sink-template
               (w/template-tap parent sink-template templatefields)
               parent)]
    (mk-cascalog-tap source sink)))

(defn hfs-tap
  "Returns a Cascading Hfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:sinkmode` - can be `:keep`, `:update` or `:replace`.

  `:sinkparts` - used to constrain the segmentation of output files.

  `:source-pattern` - Causes resulting tap to respond as a GlobHfs tap
  when used as source.

  `:sink-template` - Causes resulting tap to respond as a TemplateTap when
  used as a sink.

  `:templatefields` - When pattern is supplied via :sink-template, this option allows a
  subset of output fields to be used in the naming scheme."
  [scheme path-or-file & {:keys [sinkmode sinkparts sink-template
                                 source-pattern templatefields]
                          :or {templatefields Fields/ALL}}]
  (-> scheme
      (w/set-sinkparts! sinkparts)
      (patternize :hfs path-or-file sinkmode
                  sink-template source-pattern templatefields)))

(defn lfs-tap
  "Returns a Cascading Lfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:sinkmode` - can be `:keep`, `:update` or `:replace`.

  `:sinkparts` - used to constrain the segmentation of output files.

  `:source-pattern` - Causes resulting tap to respond as a GlobHfs tap
  when used as source.

  `:sink-template` - Causes resulting tap to respond as a TemplateTap when
  used as a sink.

  `:templatefields` - When pattern is supplied via :sink-template, this option allows a
  subset of output fields to be used in the naming scheme."
  
  [scheme path-or-file & {:keys [sinkmode sinkparts sink-template
                                 source-pattern templatefields]
                          :or {templatefields Fields/ALL}}]
  (-> scheme
      (w/set-sinkparts! sinkparts)
      (patternize :lfs path-or-file sinkmode
                  sink-template source-pattern templatefields)))
