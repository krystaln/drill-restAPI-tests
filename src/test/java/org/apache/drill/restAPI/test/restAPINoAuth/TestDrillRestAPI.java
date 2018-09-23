package org.apache.drill.restAPI.test;

import io.restassured.RestAssured;
import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;
import org.testng.annotations.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import io.restassured.response.*;
import java.util.*;
import io.restassured.path.json.*;
import io.restassured.config.*;
import io.restassured.filter.log.*;
import java.lang.String;
import java.text.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class TestDrillRestAPI {

    static String baseHost;
    private String queryId_1;
    static PrintStream fileOutPutStream = null;

    @Parameters({"unsecure_server.port","unsecure_server.host", "logDir"})
    @BeforeClass
    public static void setProperties(String drillPort, String drillHost, String logDir) throws Exception {
        String port = drillPort;
        if (port == null) {
            RestAssured.port = Integer.valueOf(8047);
        }
        else{
            RestAssured.port = Integer.valueOf(port);
        }

        baseHost = drillHost;
        if(baseHost==null){
            baseHost = "http://localhost";
        }
        RestAssured.baseURI = baseHost;

        String logPath = logDir;
        Path path = Paths.get(logPath);
        if (!Files.exists(path)) {
            
            Files.createDirectory(path);
        }    
 
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-hh:mm:SS");
        fileOutPutStream = new PrintStream(new File(logPath + "/TestDrillRestAPI_" + df.format(new Date()) + ".log"));
        RestAssured.config = config().logConfig(new LogConfig().enableLoggingOfRequestAndResponseIfValidationFails(LogDetail.ALL).defaultStream(fileOutPutStream));
    }
 
    @AfterClass
    public static void cleanUp() throws Exception {
      fileOutPutStream.close();  
    }

   @Parameters({"drillVersion"})
   @Test(groups = { "noAuth" })
   public void getClusterJson(String drillVersion) {
      given().when().get("/cluster.json").then().statusCode(200).body("drillbits.userPort", hasItem("31010")).body("drillbits.controlPort", hasItem("31011")).body("drillbits.dataPort", hasItem("31012")).body("drillbits[0].current", equalTo(true)).body("drillbits.versionMatch", everyItem(equalTo(true))).body("currentVersion", equalTo(drillVersion)).body("userEncryptionEnabled", equalTo(false)).body("bitEncryptionEnabled", equalTo(false)).body("authEnabled", equalTo(false));
   }

   @Test(groups = { "noAuth" })
   public void getStatusJson() {
      given().when().get("/status.json").then().statusCode(200).body("status", equalTo("Running!"));
   }

   @Test(groups = { "noAuth" })
   public void testQueryPage() {
      given().when().get("/query").then().statusCode(200).body(containsString("SELECT * FROM cp.`employee.json` LIMIT 20")).body(allOf(containsString("SQL"),containsString("PHYSICAL"),containsString("LOGICAL"),containsString("Query"),containsString("Submit")));
   }

   @Test(groups = { "noAuth" })
   public void testStoragePage() {
      given().when().get("/storage.json").then().statusCode(200).body("name", hasItems("cp","dfs","hive","kafka","kudu","mongo","s3"));
   }

   @Test(groups = { "noAuth" })
   public void testStorageStoragePlugins() {
      //Get the response
      Response response = get("/storage.json").then().extract().response();
      //Extract the plugin names from the response
      List<String> pluginNames = response.jsonPath().getList("name");
      //Iterate through the pluginNames
      int i;
      for(i=0;i<=pluginNames.size();i++) {
         String pluginName = response.jsonPath().param("i",i).getString("name[i]");
         //Skip plugins with null names
         if(pluginName == null) {
            continue;
         } 
         else if(pluginName.equalsIgnoreCase("cp") || pluginName.equalsIgnoreCase("dfs")) {
            given().pathParam("pluginName", pluginName).when().get("/storage/{pluginName}.json").then().statusCode(200).body("config.enabled", is(true)).body("config.type", equalTo("file")).body("config.connection",containsString(":///"));
         }
         else if(pluginName.equalsIgnoreCase("kudu") || pluginName.equalsIgnoreCase("mongo") || pluginName.equalsIgnoreCase("s3")) {
            given().pathParam("pluginName", pluginName).when().get("/storage/{pluginName}.json").then().statusCode(200).body("config.enabled", is(false));
         }
      }
   }

   @Parameters({"addNewPluginFile"})
   @Test(groups = { "noAuth" })
   public void addNewPlugin(String addNewPluginFile) throws Exception {
      File file = new File(addNewPluginFile);
      given().body(file).with().contentType("application/json").when().post("/storage/testPlugin.json").then().statusCode(200).body(containsString("success"));
      
    }

   @Test(groups = { "noAuth" },dependsOnMethods = { "addNewPlugin" })
   public void getTestPlugin() {
      given().when().get("/storage/testPlugin.json").then().statusCode(200).body("name", equalTo("testPlugin")).body("config.enabled", equalTo(false)).body("config.workspaces.drillTestDirP1.location", equalTo("/drill/testdata/p1tests"));
   }

   //Disable due to DRILL-6306-Should not be able to run queries against disabled storage plugins
   @Parameters({"queryTestPluginFile"})
   @Test(enabled = false, dependsOnMethods = { "addNewPlugin" })
   public void queryTestPlugin(String queryTestPluginFile) throws Exception {
      File file = new File(queryTestPluginFile);
      given().body(file).with().contentType("application/json").when().post("/query.json").then().statusCode(500).body("errorMessage",containsString("Table 'testPlugin.drillTestDirP1.voter' not found"));
    }

   @Parameters({"updateTestPluginFile"}) 
   //@Test(groups = { "noAuth" },dependsOnMethods = { "queryTestPlugin" })  //use this after DRILL-6306 is fixed.
   @Test(groups = { "noAuth" },dependsOnMethods = {"addNewPlugin" })  //Use this dependency for now due to DRILL-6306
   public void updateTestPlugin(String updateTestPluginFile) throws Exception {
      File file = new File(updateTestPluginFile);
      given().body(file).with().contentType("application/json").when().post("/storage/testPlugin.json").then().statusCode(200).body(containsString("success"));
   }

   @Test(groups = { "noAuth" },dependsOnMethods = { "updateTestPlugin" })
   public void checkUpdatedTestPlugin() {
     given().when().get("/storage/testPlugin.json").then().statusCode(200).body("config.enabled", is(true)).root("config.workspaces").body("drillTestDirP1.writable", is(true)).body("testSchema.location", equalTo("/drill/testdata"));
   }

   @Parameters({"queryTestPluginFile"})
   @Test(groups = { "noAuth" },dependsOnMethods = { "updateTestPlugin" })
   public void queryUpdatedTestPlugin(String queryTestPluginFile) throws Exception {
      File file = new File(queryTestPluginFile);
      given().body(file).with().contentType("application/json").when().post("/query.json").then().statusCode(200).body("columns[0]", equalTo("total_cnt")).body("rows.total_cnt[0]",equalTo("1000"));
      //Get the response
      Response response = get("/profiles.json").then().extract().response();
      //Extract the entries for finishedQueries from the response
      List<String> jsonResponse = response.jsonPath().getList("finishedQueries");
      //Iterate through the repsonse to find the matching finished query
      int i;
      for(i=0;i<=jsonResponse.size();i++) {
        String query = queryTestPluginFile;
        if (query.equalsIgnoreCase(response.jsonPath().param("i",i).getString("finishedQueries.query[i]"))) {
          break;
        }
        break;
      }
      queryId_1 = response.jsonPath().param("i",i).getString("finishedQueries.queryId[i]");
   }
 
   @Test(groups = { "noAuth" },dependsOnMethods = { "queryUpdatedTestPlugin" })
   public void verifyTestQueryProfileInfo() {
      given().pathParam("queryID", queryId_1).when().get("/profiles/{queryID}.json").then().statusCode(200).body("state",equalTo(2)).body("query", equalTo("select count(*) as total_cnt from `testPlugin.drillTestDirP1`.voter")).body("plan", containsString("Screen")).body("plan", containsString("Project")).body("plan",containsString("DirectScan")).body("plan",containsString("rowcount = 1.0")).body("plan",containsString("cumulative cost")).body("state", equalTo(2)).body("totalFragments",equalTo(1)).body("fragmentProfile.minorFragmentProfile.minorFragmentId.flatten()", everyItem(equalTo(0))).body("fragmentProfile.minorFragmentProfile.operatorProfile.peakLocalMemoryAllocated.flatten()",everyItem(greaterThan(0))).body("fragmentProfile.minorFragmentProfile.operatorProfile.operatorId.flatten()",everyItem(greaterThanOrEqualTo(0))).body(containsString("lastUpdate")).body(containsString("user"));

   }

   @Test(groups = { "noAuth" },dependsOnMethods = { "queryUpdatedTestPlugin" })
   public void disableTestPlugin() {
      given().when().get("/storage/testPlugin/enable/false").then().statusCode(200).body(containsString("success"));
   }

   @Test(groups = { "noAuth" },dependsOnMethods = { "disableTestPlugin" })
   public void verifyDisabledTestPlugin() {
      given().when().get("/storage/testPlugin.json").then().statusCode(200).body("name", equalTo("testPlugin")).body("config.enabled",equalTo(false));
   }

   @Test(groups = { "noAuth" },dependsOnMethods = { "verifyDisabledTestPlugin" })
   public void deleteTestPlugin() {
      given().when().delete("/storage/testPlugin.json").then().statusCode(200).body(containsString("success"));
   }

   @Test(groups = { "noAuth" },dependsOnMethods = { "deleteTestPlugin" })
   public void checkDeleteTestPlugin() {
      given().when().get("/storage/testPlugin.json").then().statusCode(200).body("name", equalTo("testPlugin")).body(not(contains("workspaces"))).body(not(contains("type"))).body(not(contains("workspaces"))).body(not(contains("enabled")));
   }

   @Parameters({"checkRunnningQueryProfileScript","query2"})
   @Test(groups = { "noAuth" })
   public void checkRunnningQueryProfile(String checkRunnningQueryProfileScript, String sqlQuery) throws Exception {
      File file = new File(checkRunnningQueryProfileScript);
      //Run the shell script 
      Runtime.getRuntime().exec("sh -f " + file + " " + baseHost + ":" + port);
      //put 3 seconds sleep time to make sure the query is in a running state
      Thread.sleep(3000);
      Response response = get("/profiles.json").then().extract().response();
      //Extract the entries for runningQueries from the response
      List<String> jsonResponse = response.jsonPath().getList("runningQueries");
      //Iterate through the repsonse to find the matching running query
      int i;
      for(i=0;i<=jsonResponse.size();i++) {
        String query = sqlQuery;
        if (query.equalsIgnoreCase(response.jsonPath().param("i",i).getString("runningQueries.query[i]"))) {
          break;
        }
        break;
      }
      String queryId = response.jsonPath().param("i",i).getString("runningQueries.queryId[i]");
      given().pathParam("queryID", queryId).when().get("/profiles/{queryID}.json").then().statusCode(200).body("state",equalTo(1)).body("query", containsString("select count (distinct ss_customer_sk),count(distinct ss_item_sk),count(distinct ss_store_sk),max(ss_ticket_number),sum(ss_item_sk),max(ss_net_profit)")).body(containsString("majorFragmentId")).body(containsString("minorFragmentId")).body(containsString("minorFragmentProfile")).body(containsString("operatorProfile"));
   }

   @Parameters({"cancelRunningQueryFile","query3"})
   @Test(groups = { "noAuth" })
   public void cancelRunningQuery(String cancelRunningQueryFile, String sqlQuery) throws Exception {
      File file = new File(cancelRunningQueryFile);
      //Run the shell script
      Runtime.getRuntime().exec("sh -f " + file + " " + baseHost + ":" + port);
      //put 2 seconds sleep time here to make sure query is in running state
      Thread.sleep(2000);
      Response response = get("/profiles.json").then().extract().response();
      //Extract the entries for runningQueries from the response
      List<String> jsonResponse = response.jsonPath().getList("runningQueries");
      //Iterate through the repsonse to find the matching running query
      int i;
      for(i=0;i<=jsonResponse.size();i++) {
        String query = sqlQuery;
        if (query.equalsIgnoreCase(response.jsonPath().param("i",i).getString("runningQueries.query[i]"))) {
          break;
        }
        break;
      }
      String queryId = response.jsonPath().param("i",i).getString("runningQueries.queryId[i]");  
      //Cancel the query with the queryId
      given().pathParam("queryID", queryId).when().get("/profiles/cancel/{queryID}").then().statusCode(200);
      //put 10 seconds sleep time here for query to completely cancelled
      Thread.sleep(10000);
      given().pathParam("queryID", queryId).when().get("/profiles/{queryID}.json").then().statusCode(200).body("state",equalTo(3)); 
   }

   @Test(groups = { "noAuth" })
   public void getOptions() {
      given().when().get("/options.json").then().statusCode(200).body("name", hasItems("planner.width.max_per_query","drill.exec.storage.implicit.filename.column.label")).body("value", hasItems(true,false,64,"filename","DEFAULT")).body("accessibleScopes", hasItems("SYSTEM","ALL")).body("kind", hasItems("BOOLEAN","LONG","STRING","DOUBLE"));
   }

   @Test(groups = { "noAuth" })
   public void checkThreadsPage() {
      given().when().get("/status/threads").then().statusCode(200).body(allOf(containsString("Reference Handler")),containsString("Signal Dispatcher"));
   }

   @Test(groups = { "noAuth" },dependsOnMethods = { "queryUpdatedTestPlugin" })
   public void checkMetricsPage() {
     given().when().get("/status/metrics").then().statusCode(200).body("gauges.count.value", greaterThan(0)).body("gauges.'daemon.count'.value", greaterThan(0)).body("gauges.'heap.used'.value", greaterThan(0)).body("counters.'drill.connections.rpc.control.unencrypted'.count", greaterThanOrEqualTo(0)).body("counters.'drill.connections.rpc.user.unencrypted'.count", greaterThanOrEqualTo(0)).body("counters.'drill.queries.completed'.count", greaterThan(0)).body("histograms.'drill.allocator.huge.hist'.count", greaterThanOrEqualTo(0)).body("histograms.'drill.allocator.normal.hist'.max", greaterThan(0)).body("histograms.'drill.allocator.normal.hist'.stddev", greaterThan(0.0f));
   }

   @Test(groups = { "noAuth" })
   public void checkLogsPage() {
      SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
      given().when().get("/logs.json").then().statusCode(200).body("name", hasItems(equalTo("drillbit.log"),equalTo("drillbit.out"),equalTo("drillbit_queries.json"), equalTo("sqlline.log"))).body("lastModified", hasItem(containsString(df.format(new Date()))));
   }
}
