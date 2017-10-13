package cn.kepuchina.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import javax.management.Query;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Zouyy on 2017/8/17.
 */
public class demo2 {

    private TransportClient client;

    @Before
    public void before(){
        //获取es集群的连接方式
        ImmutableSettings.settingsBuilder()
                .put(
                        "cluster.name",
                        "elasticsearch"
                )//设置集群名称
                .put(
                        "client.transport.sniff",
                        true
                )//开启集群自动嗅探功能
                .build();
        client = new TransportClient()
                .addTransportAddress(
                        new InetSocketTransportAddress("hadoop7",9300)
                )
        ;
    }


    @Test
    /**
     * 连接到的节点消息
     */
    public void test1(){
        ImmutableList<DiscoveryNode> discoveryNodes = client.connectedNodes();
        for (DiscoveryNode node :discoveryNodes) {
            System.out.println(node.getHostAddress());
            System.out.println(node.name());

        }
    }


    private final  String index ="i_demo1";
    private final  String type ="emp";

    /**
     * 创建消息
     */
    @Test
    public void test2() {
        String jsonStr ="{\"name\":\"zs\",\"age\":19}";
        IndexResponse indexResponse = client.prepareIndex("i_demo", type, "1")
                .setSource(jsonStr)
                .get();
        System.out.println(indexResponse.getVersion());
    }

    /**
     * 删除消息
     */
    @Test
    public void test3(){
        System.out.println(client.prepareDelete(index,type,"1").get());
    }

    /**
     * 通过hashmap创建数据
     */
    @Test
    public void test4(){
        String[] arr = {"sports","music"};
        HashMap<String,Object> map = new HashMap<String,Object>();
        map.put("first_name","zou");
        map.put("last_name","yan");
        map.put("age",25);
        map.put("interests",arr);
        client.prepareIndex(index,type,"3").setSource(map).get();
    }

    /**
     * 通过对象进行转载数据
     * 注意：对象必须实现get/set方法
     */
    @Test
    public void test5() throws JsonProcessingException {
        Person person = new Person(
                "yang",
                "yang",
                50,
                new String[]{"jingtian","mingtian"},
                "杨洋！"
        );
        ObjectMapper objectMapper = new ObjectMapper();
        client.prepareIndex(index,type,"11")
                .setSource(objectMapper.writeValueAsString(person))
                .get();
    }

    /**
     * index-- es helper
     */
    @Test
    public void test6() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("first_name", "he")
                .field("last_name", "baojia")
                .field("age", 30)
                .field("interests", new String[]{"host", "windows"})
                .field("about", "开始了")
                .endObject();
        IndexResponse indexResponse = client.prepareIndex(index, type, "8")
                .setSource(builder)
                .get();
        System.out.println(indexResponse.getVersion());
    }

    /**
     * get 根据id进行查询
     */
    @Test
    public void test7(){
        GetResponse getResponse = client.prepareGet(index, type, "7").get();
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * update 局部更新
     */
    @Test
    public void test8() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("about", "更新了！")
                .endObject();
        UpdateResponse updateResponse = client.prepareUpdate(
                index,
                type,
                "8"
        ).setDoc(builder).get();
        System.out.println(updateResponse.getVersion());
    }

    /**
     * 求count
     */
    @Test
    public void test9(){
        long count = client.prepareCount(index).get().getCount();
        System.out.println(count);
    }

    /**
     * 批量处理
     * CreateIndexRequest和IndexRequest的区别
     * 如果数据存在，使用create操作失败，会提示文档已经存在，使用index则可以成功执行
     */
    @Test
    public void test10() throws IOException {
        BulkRequestBuilder bulk = client.prepareBulk();
        //插入一条数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("first_name", "fan")
                .field("last_name", "lina")
                .field("age", 25)
                .field("interests", new String[]{"network", "disk"})
                .field("about", "让我们一起努力把！").endObject();
        IndexRequest indexRequest = new IndexRequest(index, type, "10");
        indexRequest.source(builder);
        //修改
        UpdateRequest updateRequest = new UpdateRequest(index,type,"7")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("about", "更新了哦！")
                        .endObject());
        //删除
        DeleteRequest deleteRequest = new DeleteRequest(index,type, "3");
        //批量组装
        bulk.add(indexRequest);
        bulk.add(updateRequest);
        bulk.add(deleteRequest);
        //执行操作
        BulkResponse bulkResponse = bulk.get();
        //判断是否有执行失败的
        if(bulkResponse.hasFailures()){
            BulkItemResponse[] items = bulkResponse.getItems();
            for (BulkItemResponse bulkItemResponse :
                    items) {
                System.out.println(bulkItemResponse.getFailureMessage());
            }
        }else{
            System.out.println("全部执行成功！");
        }
    }

    /**
     * 查询：
     * 注意：普通的查询使用matchQuery或者multiMatch查询就足够用了
     * 针对复杂的查询，建议使用 queryString？
     * 针对不分词的查询，建议使用 termQuery
     * lt:小于
     * lte:小于等于
     * gt:大于
     * gte:大于等于
     */
    @Test
    public void test11(){

        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                //默认OR:OR--包含“任意一个字符就返回”，AND--必须包含所有的字符。
//                .setQuery(QueryBuilders.matchQuery("last_name","y").operator(MatchQueryBuilder.Operator.OR))
//                .setQuery(QueryBuilders.matchQuery("about","杨飞"))//指定条件查询，matchQuery,进行分词查询，不支持通配符！
                .setQuery(QueryBuilders.matchAllQuery())//查询所有数据
//                .setQuery(QueryBuilders.multiMatchQuery("yang","first_name","last_name"))//根据多个字段进行查询
//                .setQuery(QueryBuilders.termQuery("about","杨飞"))//查询的时候不分词，针对 人名、地名或者特殊的内容
                .setSearchType(SearchType.QUERY_THEN_FETCH)//指定查询方式
                //设置分页
                .setFrom(0)//开始角标
                .setSize(10)//返回数据的总条数
                //排序
                .addSort("age", SortOrder.ASC)
                .setPostFilter(FilterBuilders.rangeFilter("age").from(25).to(50).includeLower(true).includeUpper(false))
                .setExplain(true)
                .addHighlightedField("about")//设置高亮字段
                .setHighlighterPreTags("<font color='red'>")//设置高亮字段的前缀
                .setHighlighterPostTags("</font>")//设置高亮字段的后缀
                .get();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数"+totalHits);
        for (SearchHit searchHit :
                hits) {
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("about");
            if (highlightField!=null){
                Text[] fragments = highlightField.getFragments();
                System.out.println(fragments[0]);
            }
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 分组统计总数
     * select age,count(1) from users group by age
     */
    @Test
    public void test12(){
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(AggregationBuilders.terms("age_term").field("age").size(0))
                .get();
        Terms term = searchResponse.getAggregations().get("age_term");
        List<Terms.Bucket> buckets = term.getBuckets();
        for (Terms.Bucket bucket :
                buckets) {
            System.out.println(bucket.getKey()+"---"+bucket.getDocCount());
        }
    }

    /**
     *sum求和
     * select sum(age) from user group by first_name
     */
    @Test
    public void test13(){
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("name_term").field("first_name")
                        .subAggregation(AggregationBuilders.sum("avg_age").field("age"))
                )
                .get();
        Terms terms = searchResponse.getAggregations().get("name_term");
        List<Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket :
                buckets) {
            String key = bucket.getKey();
            Sum sum= bucket.getAggregations().get("avg_age");
            System.out.println(key+"---"+sum.getValue());
        }
    }

    /**
     * 不分组求平均值
     */
    @Test
    public void test14(){
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
//                        AggregationBuilders.avg("age_term").field("age")
                        AggregationBuilders.sum("age_term").field("age")
                )
                .get();
//        Avg avg= searchResponse.getAggregations().get("age_term");
        Sum sum = searchResponse.getAggregations().get("age_term");
        System.out.println(sum.getName()+"----"+ sum.getValue());

    }

    /**
     * 清空索引库里面的数据
     */
    @Test
    public void test15(){
        client.prepareDeleteByQuery("i_demo")
            .setTypes("emp")
            .setQuery(QueryBuilders.matchAllQuery())
            .get();
    }

    /**
     * nbl,要删除索引库了
     */
    @Test
    public void test16(){
        client.admin().indices().prepareDelete("hadoop").get();
    }
}
