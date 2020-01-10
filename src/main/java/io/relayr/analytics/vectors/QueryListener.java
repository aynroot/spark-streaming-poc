package io.relayr.analytics.vectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.HashMap;

public class QueryListener extends StreamingQueryListener {

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        // should be executed every time micro-batch is completed
        HashMap<String, HashMap<Integer, Long>> offsets = new Gson().fromJson(event.progress().sources()[0].endOffset(),
                new TypeToken<HashMap<String, HashMap<Integer, Long>>>() {
                }.getType());

        System.out.println("Offset for partition 8: " + offsets.get("test").get(8));
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        System.out.println("onQueryStarted");
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
    }
}
