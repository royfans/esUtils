package esutiltest;

import com.course.utils.ESUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Administrator on 2019/6/26.
 */
public class ESUtilsTest {

    public static TransportClient client;

    /**
     * test create index
     */
    @Test
    @SuppressWarnings("unused")
    private void createIndex() {
        client = new ESUtils().getClient();
        try {
            //create index
            //title:field name，  type:content type       analyzer ：analyzer type
            XContentBuilder mapping = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("title").field("type", "text").field("analyzer", "ik_smart").endObject()
                    .startObject("content").field("type", "text").field("analyzer", "ik_max_word").endObject()
                    .endObject()
                    .endObject();
            //index：index name   type：type name
            PutMappingRequest putmap = Requests.putMappingRequest("yzy").type("yzy").source(mapping);
            //create index
            client.admin().indices().prepareCreate("yzy").execute().actionGet();
            //index add mapping
            client.admin().indices().putMapping(putmap).actionGet();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * add data
     * @throws IOException
     */
    @Test
    public void addIndex()throws IOException{
        client = new ESUtils().getClient();
        try{
            //prepareIndex(索引,文本类型,ID)
            IndexResponse response = client.prepareIndex("yzy", "type", "2")
                    .setSource(jsonBuilder()
                                    .startObject()
                                    .field("title", "标题")   //field/value
                                    .field("content", "内容")
                                    .endObject()
                    ).get();
            System.out.println(response.toString());

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * update index
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void updateByClient() throws IOException, ExecutionException, InterruptedException {
        client = new ESUtils().getClient();

        UpdateResponse response = client.update(new UpdateRequest("yzy", "type", "1")
                .doc(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("title", "标题1")
                                .field("content","Hello world!!!")
                                .endObject()
                )).get();
        System.out.println(response.toString());
    }

    /**
     * Get the specified document
     * @param index
     * @param type
     * @param idtype
     * @return
     */
    public Map<String,Object> get(String index,String type,String idtype){
        client = new ESUtils().getClient();
        GetResponse response = client.prepareGet(index, type,idtype).get();
        Map<String, Object> source = response.getSource();
        Set<String> strings = source.keySet();
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()) {
            System.out.println(source.get(iterator.next()));
        }
        return source;
    }

    /**
     * Get the specified document due to id
     */
    @Test
    public void get() {
        String index = "yzy";
        String type = "type";
        String id = "1";
        Map source = new ESUtilsTest().get(index,type,id);
        System.out.println(source);
    }

    /**
     * 根据 queue 查询
     * @param index
     * @param type
     * @param query
     * @param cxz
     * @return
     */
    public List<Map<String,Object>> search(String index,String type,String query,String cxz){
        client = new ESUtils().getClient();
        final  List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        SearchResponse sr = client.prepareSearch()  //指定多个索引
                .setTypes(index, type)  //指定类型
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery(query, cxz))  // Query
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        long totalHits1= sr.getHits().totalHits;  //命中个数
        for (SearchHit searchHit : sr.getHits().getHits()) {
            final Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            list.add(sourceAsMap);
        }
        System.out.println(totalHits1+"---"+sr.toString());
        return list;
    }

    /**
     * delete index
     */
    @Test
    public void delete(){
        client = new ESUtils().getClient();
//        DeleteResponse response = client.prepareDelete("yzy", "type", "1").get();  //删除文档
//        System.out.println(response.toString());
        //删除索引
        DeleteIndexResponse deleteIndexResponse = client.admin().indices()
                .prepareDelete("yzy")
                .execute().actionGet();
        boolean isFound = deleteIndexResponse.isAcknowledged();
        System.out.println(isFound);//返回文档是否存在，存在删除
    }
    public void bulkdoc() throws IOException {
        client = new ESUtils().getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "trying out Elasticsearch")
                                        .endObject()
                        )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "another post")
                                        .endObject()
                        )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }

    }

    public List<Map<String,Object>> searchZd(String query,String msg){
        client = new ESUtils().getClient();
        Map<String, Object> template_params = new HashMap<String, Object>();
        template_params.put("param_gender", msg);
        template_params.put("param_query", query);
        final  List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        //模板查询
        SearchResponse sr = new SearchTemplateRequestBuilder(client)
                .setScript("{\n" +
                        "        \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "                \"{{param_query}}\" : \"{{param_gender}}\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "}")
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();
        long totalHits1= sr.getHits().totalHits;  //命中个数
        for (SearchHit searchHit : sr.getHits().getHits()) {
            final Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            list.add(sourceAsMap);
        }
        System.out.println(totalHits1+"---"+sr.toString());
        return list;
    }

    /**
     * bool 查询
     */
    @Test
    public void search2(){
        client = new ESUtils().getClient();
        // 添加bool search 条件
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("field", "280"))
                .must(QueryBuilders.matchQuery("attention","0"))
                .must(QueryBuilders.rangeQuery("number").gte("863273032001000").lte("863273032001999"));
        // source
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
//                .fetchSource(new String[]{"account_name"}, new String[]{"place_id"});
                //include && enclude
                .fetchSource(new String[]{"account", "attention"}, Strings.EMPTY_ARRAY);


        // search
        SearchResponse searchResponse = client.prepareSearch("tb_small_account_v3_test")
                .setTypes("tb_small_account_v3_test")
                .setSource(searchSourceBuilder) //add source
                .setQuery(queryBuilder) //add bool search
                .setFrom(0)  //search start
                .setSize(1000) //search end
                .get();
        for(SearchHit searchHit:searchResponse.getHits().getHits()){
            System.out.println(searchHit.getSourceAsString());
        }
//        System.out.println(searchResponse.toString());
    }

    /**
     * match seach
     */
    @Test
    public void matchSearch(){
        client = new ESUtils().getClient();
        // 添加bool search 条件
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("field", "280");

        // source
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
//                .fetchSource(new String[]{"account"}, new String[]{"place"});
                .fetchSource(new String[]{"account", "operation"}, Strings.EMPTY_ARRAY);

        // 查询
        SearchResponse searchResponse = client.prepareSearch("test")
                .setTypes("test")
                .setSource(searchSourceBuilder) //添加source
                .setQuery(queryBuilder) //添加 bool 查询
                .addSort("operation", SortOrder.DESC)
                .setFrom(0)  //查询起始
                .setSize(1000) //查询结束
                .get();
        for(SearchHit searchHit:searchResponse.getHits().getHits()){
            System.out.println(searchHit.getSourceAsString());
        }
//        System.out.println(searchResponse.toString());
    }

}
