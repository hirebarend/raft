import { Log } from './log';

export class LogArrayHelper {
  public static getLastIndex(arr: Array<Log>): number {
    if (arr.length === 0) {
      return 0;
    }

    return arr[arr.length - 1].index;
  }

  public static getTerm(arr: Array<Log>, index: number): number {
    const item: Log | undefined = arr.find((x) => x.index === index);

    if (!item) {
      return 0;
    }

    return item.term;
  }

  public static slice(arr: Array<Log>, index: number): Array<Log> {
    return arr.filter((x) => x.index >= index);
  }
}
