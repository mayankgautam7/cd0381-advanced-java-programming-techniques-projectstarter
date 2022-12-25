package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParallelTaskCrawl extends RecursiveTask
{
    private final String url;
    private final Clock clock;
    private final PageParserFactory parserFactory;
    private final Instant deadline;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;

    private final Map<String, Integer> countMap;
    private final ConcurrentSkipListSet<String> visitedUrls;

    public ParallelTaskCrawl(String url, Clock clock, PageParserFactory parserFactory, Instant deadline, int maxDepth, List<Pattern> ignoredUrls, Map<String, Integer> countMap, ConcurrentSkipListSet<String> visitedUrls) {
        this.url = url;
        this.clock = clock;
        this.parserFactory = parserFactory;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.ignoredUrls = ignoredUrls;
        this.countMap = countMap;
        this.visitedUrls = visitedUrls;
    }

    @Override
    protected Boolean compute() {
        if(maxDepth == 0 || clock.instant().isAfter(deadline))
        {
            return false;
        }

        for(Pattern pattern : ignoredUrls)
        {
            if(pattern.matcher(url).matches())
            {
                return false;
            }
        }

        if(visitedUrls.contains(url))
        {
            return false;
        }
        visitedUrls.add(url);
        PageParser.Result result = parserFactory.get(url).parse();

        for(Map.Entry<String, Integer> e : result.getWordCounts().entrySet())
        {
            countMap.compute(e.getKey(),(k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
        }

        List<ParallelTaskCrawl> subtasks = result.getLinks()
                .stream()
                .map(link -> new Builder()
                        .setClock(clock)
                        .setParserFactory(parserFactory)
                        .setDeadline(deadline)
                        .setCountMap(countMap)
                        .setIgnoredUrls(ignoredUrls)
                        .setUrl(link)
                        .setMaxDepth(maxDepth-1)
                        .setVisitedUrls(visitedUrls)
                        .build())
                .collect(Collectors.toList());

        invokeAll(subtasks);
        return true;
    }

    public static final class Builder
    {
        private String url;
        private Clock clock;
        private PageParserFactory parserFactory;
        private Instant deadline;
        private int maxDepth;
        private List<Pattern> ignoredUrls;

        private Map<String, Integer> countMap;
        private ConcurrentSkipListSet<String> visitedUrls;

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setParserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public Builder setCountMap(Map<String, Integer> countMap) {
            this.countMap = countMap;
            return this;
        }

        public Builder setVisitedUrls(ConcurrentSkipListSet<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public ParallelTaskCrawl build()
        {
            return new ParallelTaskCrawl(url,clock,parserFactory,deadline,maxDepth,ignoredUrls,countMap,visitedUrls);
        }
    }
}
