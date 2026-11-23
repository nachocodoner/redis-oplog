import { Events, Strategy } from "../constants";

import defaultStrategy from "./default";
import directStrategy from "./direct";
import limitSortStrategy from "./limit-sort";
import getStrategy from "./getStrategy";
import requery from "./actions/requery";

const StrategyProcessorMap = {
  [Strategy.LIMIT_SORT]: limitSortStrategy,
  [Strategy.DEFAULT]: defaultStrategy,
  [Strategy.DEDICATED_CHANNELS]: directStrategy,
};

export { getStrategy };

/**
 * @param strategy
 * @param options cursor options after collection defaults have been applied
 * @returns {*}
 */
export function getProcessor(strategy, options = {}) {
  if (options.skip > 0 && !options.limit) {
    // A write to a skipped document can move another document across the boundary.
    return (collection, event, doc, fields = []) =>
      requery(collection, event === Events.REMOVE ? null : doc, event, fields);
  }
  return StrategyProcessorMap[strategy];
}
