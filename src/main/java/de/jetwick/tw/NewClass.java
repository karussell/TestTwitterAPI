/*
 *  Copyright 2011 Peter Karich, jetwick_@_pannous_._info.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package de.jetwick.tw;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.http.AccessToken;

/**
 *
 * @author Peter Karich, jetwick_@_pannous_._info
 */
public class NewClass {

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.out.println("usage\njava -Dtwitter4j.oauth.consumerKey=key -Dtwitter4j.oauth.consumerSecret=val "
                    + "de.jetwick.tw.NewClass \"term1 term2\" token tokenSecure");
            return;
        }
        final String queryTerms = args[0];
        final String token = args[1];
        final String tokenSecret = args[2];   
        System.out.println("query terms are " + queryTerms);
        final NewClass test = new NewClass(queryTerms);
        Runnable runOnExit = new Runnable() {

            @Override
            public void run() {
                test.calc();
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(runOnExit));
        Thread t = test.search();
        Thread t2 = test.streaming(token, tokenSecret);

        t.start();
        t2.start();

        t.join();
        t2.join();
    }
    private String queryTerms;

    public NewClass(String query) {
        this.queryTerms = query;
    }
    private Map<Long, String> searchMap = new TreeMap<Long, String>();
    private Map<Long, String> asyncMap = new TreeMap<Long, String>();

    private void error(String str) {
        System.out.println(new Date() + " ERROR:" + str);
    }

    private void log(String str) {
        System.out.println(new Date() + "  INFO:" + str);
    }
    
    /** A thread using the search API */
    public Thread search() {
        return new Thread() {

            @Override
            public void run() {
                int MINUTES = 2;
                Twitter twitter = new TwitterFactory().getInstance();
                try {
                    while (!isInterrupted()) {
                        Query query = new Query(queryTerms);
                        // RECENT or POPULAR or MIXED
                        // doesn't make a difference if MIXED or RECENT
                        query.setResultType(Query.MIXED);
                        query.setPage(1);
                        query.setRpp(100);
                        QueryResult res = twitter.search(query);
                        for (Tweet tw : res.getTweets()) {
                            searchMap.put(tw.getId(), tw.getText());
                        }
                        Thread.sleep(MINUTES * 60 * 1000L);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

    }

    /**
     * A thread using the streaming API.
     * Maximum tweets/sec == 50 !! we'll lose even infrequent tweets for a lot keywords!
     */
    public Thread streaming(final String token, final String tokenSecret) {
        return new Thread() {

            StatusListener listener = new StatusListener() {

                int counter = 0;
                public void onStatus(Status status) {
                    if (++counter % 20 == 0)
                        log("async counter=" + counter);
                    asyncMap.put(status.getId(), status.getText());
                }

                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    log("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                }

                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    log("Got track limitation notice:" + numberOfLimitedStatuses);
                }

                public void onScrubGeo(int userId, long upToStatusId) {
                    log("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };

            public void run() {
                TwitterStream twitterStream = new TwitterStreamFactory().getInstance(new AccessToken(token, tokenSecret));
                twitterStream.addListener(listener);
                twitterStream.filter(new FilterQuery(0, null, new String[]{queryTerms}, null));
            }
        };
    }

    /**
     * Calculates which tweets are missing from search and which are missing from streaming API
     */
    public void calc() {
        if (asyncMap.isEmpty()) {
            log("async is empty");
            return;
        }
        long minId = Collections.min(asyncMap.keySet());
        Map<Long, String> all = new TreeMap<Long, String>(asyncMap);
        Map<Long, String> searchMapWithoutHistoric = new TreeMap<Long, String>();
        for (Entry<Long, String> e : searchMap.entrySet()) {
            // streaming does not catch historic tweets, so do not include them
            if (e.getKey() < minId)
                continue;

            searchMapWithoutHistoric.put(e.getKey(), e.getValue());
            String str = all.put(e.getKey(), e.getValue());
            if (str != null && !str.equals(e.getValue()))
                error(e.getKey() + " strings different:" + str + " != " + e.getValue());
        }

        Map<Long, String> onlyInSearch = new TreeMap<Long, String>(searchMapWithoutHistoric);
        for (Long id : asyncMap.keySet()) {
            onlyInSearch.remove(id);
        }
        Map<Long, String> onlyInAsync = new TreeMap<Long, String>(asyncMap);
        for (Long id : searchMapWithoutHistoric.keySet()) {
            onlyInAsync.remove(id);
        }

        log("async :" + asyncMap.size());
        log("search:" + searchMapWithoutHistoric.size());
        log("all   :" + all.size());

        String str = "";
        for (Entry<Long, String> e : onlyInSearch.entrySet()) {
            str += toString(e);
        }
        log("### only in search ###\n" + str);

        str = "";
        for (Entry<Long, String> e : onlyInAsync.entrySet()) {
            str += toString(e);
        }
        log("### only in async ###\n" + str);
    }

    private String toString(Entry<Long, String> entry) {
        return entry.getKey() + " " + entry.getValue().replaceAll("\n", " ") + "\n";
    }   
}
