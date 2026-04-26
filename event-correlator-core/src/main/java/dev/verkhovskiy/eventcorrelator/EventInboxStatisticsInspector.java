package dev.verkhovskiy.eventcorrelator;

/** API только для чтения, который возвращает сводное состояние durable inbox. */
public interface EventInboxStatisticsInspector {

  /** Считает агрегированную статистику durable inbox по заданному фильтру. */
  EventInboxStatistics getStatistics(EventInboxStatisticsQuery query);
}
