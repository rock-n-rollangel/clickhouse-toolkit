export type Engine =
  | 'MergeTree'
  | 'AggregatingMergeTree'
  | 'ReplacingMergeTree'
  | 'SummingMergeTree'
  | 'CollapsingMergeTree'
  | 'VersionedCollapsingMergeTree'
  | 'GraphiteMergeTree'
  | 'TinyLog'
  | 'StripeLog'
  | 'Log'
