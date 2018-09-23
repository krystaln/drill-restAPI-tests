package org.apache.drill.restAPI.test;

import io.restassured.RestAssured;
import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import io.restassured.authentication.*;
import static org.hamcrest.Matchers.*;
import org.testng.annotations.*;
import io.restassured.response.*;
import io.restassured.path.json.*;
import io.restassured.config.*;
import io.restassured.filter.log.*;
import io.restassured.http.ContentType;
import io.restassured.builder.*;
import io.restassured.filter.session.SessionFilter;
import java.lang.String;
import java.text.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestDrillRestAPIAuth {

    static String baseHost;
    //private String drillbit = "perfnode207.perf.lab";  ////Remove this after DRILL-6693 is fixed
    static PrintStream fileOutPutStream = null;
    static SessionFilter nonAdmin1sessionFilter = null;
    static SessionFilter nonAdmin2sessionFilter = null;
    static SessionFilter adminSessionFilter = null;


    @Parameters({"server.port","server.host","nonAdmin1UserName","nonAdmin1Passwd","nonAdmin2UserName","nonAdmin2Passwd","adminUserName","adminPasswd", "logDir"})
    @BeforeClass
    public static void setProperties(String drillPort, String drillHost, String nonAdmin1UserName, String nonAdmin1Passwd, String nonAdmin2UserName, String nonAdmin2Passwd, String adminUserName, String adminPasswd, String logDir) throws Exception {
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

        nonAdmin1sessionFilter = new SessionFilter(); 
        RestAssured.given().auth().form(nonAdmin1UserName, nonAdmin1Passwd, new FormAuthConfig("/j_security_check", "j_username", "j_password")).filter(nonAdmin1sessionFilter).when().get("/login"); 
  
        nonAdmin2sessionFilter = new SessionFilter();
        RestAssured.given().auth().form(nonAdmin2UserName, nonAdmin2Passwd, new FormAuthConfig("/j_security_check", "j_username", "j_password")).filter(nonAdmin2sessionFilter).when().get("/login");

        adminSessionFilter = new SessionFilter();
        RestAssured.given().auth().form(adminUserName, adminPasswd, new FormAuthConfig("/j_security_check", "j_username", "j_password")).filter(adminSessionFilter).when().get("/login");

        String logPath = logDir;
        Path path = Paths.get(logPath);
        if (!Files.exists(path)) {

            Files.createDirectory(path);
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-hh:mm:SS");
        fileOutPutStream = new PrintStream(new File(logPath + "/TestDrillRestAPIAuth_" + df.format(new Date()) + ".log"));
        RestAssured.config = config().logConfig(new LogConfig().enableLoggingOfRequestAndResponseIfValidationFails(LogDetail.ALL).defaultStream(fileOutPutStream));
    }

    @AfterClass
    public static void cleanUp() throws Exception {
      fileOutPutStream.close();
    }

    @Parameters({"invalidUserName", "validPasswd"})
    @Test(groups = { "LoggedIn" })
    public void invalidUserName(String invalidUserName, String validPasswd) {
       given().auth().form(invalidUserName, validPasswd, new FormAuthConfig("/j_security_check", "j_username", "j_password")).when().get("/login").then().body(containsString("Log In")).body(containsString("Log In to Drill Web Console"));
    }

    @Parameters({"nonAdmin1UserName", "invalidPasswd"})
    @Test(groups = { "LoggedIn" })
    public void invalidPasswd(String nonAdmin1UserName, String invalidPasswd) {
       given().auth().form(nonAdmin1UserName, invalidPasswd, new FormAuthConfig("/j_security_check", "j_username", "j_password")).when().get("/login").then().body(containsString("Log In")).body(containsString("Log In to Drill Web Console"));
    }    

    @Parameters({"nonAdmin1UserName", "nonAdmin1Passwd"})
    @Test(groups = { "LoggedIn" })
    public void validUserPass(String nonAdmin1UserName, String nonAdmin1Passwd) {
       given().auth().form(nonAdmin1UserName, nonAdmin1Passwd, new FormAuthConfig("/j_security_check", "j_username", "j_password")).when().get("/login").then().body(containsString("Log Out" + " (" + nonAdmin1UserName + ")"));
    }
 
    @Parameters({"nonAdmin1UserName"})
    @Test(groups = { "LoggedIn" })
    public void nonAdminHomePage(String nonAdmin1UserName) {
       given().filter(nonAdmin1sessionFilter).when().get("/login").then().statusCode(200).body(containsString("Query")).body(containsString("Apache Drill")).body(containsString("Profiles")).body(containsString("Metrics")).body(containsString("Documentation")).body(containsString("Log Out" + " (" + nonAdmin1UserName + ")")).body(containsString("Client to Bit Encryption")).body(containsString("Bit to Bit Encryption")).body(containsString("Queue Status")).body(not(containsString("Options"))).body(not(containsString("Storage"))).body(not(containsString("Threads"))).body(not(containsString("Logs"))).body(not(containsString("SHUTDOWN"))).body(not(containsString("User Info")));
    }

    @Parameters({"adminUserName"})
    @Test(groups = { "LoggedIn" })
    public void adminHomePage(String adminUserName) {
       given().filter(adminSessionFilter).when().get("/login").then().statusCode(200).body(containsString("Query")).body(containsString("Apache Drill")).body(containsString("Profiles")).body(containsString("Metrics")).body(containsString("Documentation")).body(containsString("Storage")).body(containsString("Options")).body(containsString("Threads")).body(containsString("Logs")).body(containsString("Shutdown")).body(containsString("Log Out" + " (" + adminUserName + ")")).body(containsString("Client to Bit Encryption")).body(containsString("Bit to Bit Encryption")).body(containsString("Admin Users")).body(containsString("Admin User Groups")).body(containsString("Process User")).body(containsString("Process User Groups"));
    }

    @Test(groups = { "LoggedIn" })
    public void nonAdminAccessQueryPage() {
      given().filter(nonAdmin1sessionFilter).when().get("/query").then().statusCode(200).body(containsString("SELECT * FROM cp.`employee.json` LIMIT 20")).body(allOf(containsString("SQL"),containsString("PHYSICAL"),containsString("LOGICAL"),containsString("Query"),containsString("Submit")));
   }
 
    @Test(groups = { "LoggedIn" })
    public void adminAccessQueryPage() {
      given().filter(adminSessionFilter).when().get("/query").then().statusCode(200).body(containsString("SELECT * FROM cp.`employee.json` LIMIT 20")).body(allOf(containsString("SQL"),containsString("PHYSICAL"),containsString("LOGICAL"),containsString("Query"),containsString("Submit")));
    }

    @Test(groups = { "LoggedIn" })
    public void nonAdminStoragePage() {
       given().filter(nonAdmin2sessionFilter).get("/storage.json").then().statusCode(500).body(containsString("HTTP 403 Forbidden"));
    }
       
    @Test(groups = { "LoggedIn" })
    public void adminStoragePage() {
       //Get the response
       Response response = given().filter(adminSessionFilter).get("/storage.json").then().extract().response();
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
             given().pathParam("pluginName", pluginName).filter(adminSessionFilter).when().get("/storage/{pluginName}.json").then().statusCode(200).body("config.enabled", is(true)).body("config.type", equalTo("file")).body("config.connection",containsString(":///"));
          }
          else if(pluginName.equalsIgnoreCase("kudu") || pluginName.equalsIgnoreCase("mongo") || pluginName.equalsIgnoreCase("s3")) {
             given().pathParam("pluginName", pluginName).filter(adminSessionFilter).when().get("/storage/{pluginName}.json").then().statusCode(200).body("config.enabled", is(false));
          }
       }
    }

    @Test(groups = { "LoggedIn" }, dependsOnMethods = {"adminRunQuery"})
    public void nonAdminMetricsPage() {
    given().filter(nonAdmin1sessionFilter).when().get("/status/metrics").then().statusCode(200).body("gauges.count.value", greaterThan(0)).body("gauges.'daemon.count'.value", greaterThan(0)).body("gauges.'heap.used'.value", greaterThan(0)).body("counters.'drill.connections.rpc.control.unencrypted'.count", greaterThanOrEqualTo(0)).body("counters.'drill.connections.rpc.user.unencrypted'.count", greaterThanOrEqualTo(0)).body("counters.'drill.queries.completed'.count", greaterThan(0)).body("histograms.'drill.allocator.huge.hist'.count", greaterThanOrEqualTo(0));
   }

   @Test(groups = { "LoggedIn" }, dependsOnMethods = {"adminRunQuery"})
    public void adminMetricsPage() {
    given().filter(adminSessionFilter).when().get("/status/metrics").then().statusCode(200).body("gauges.count.value", greaterThan(0)).body("gauges.'daemon.count'.value", greaterThan(0)).body("gauges.'heap.used'.value", greaterThan(0)).body("counters.'drill.connections.rpc.control.unencrypted'.count", greaterThanOrEqualTo(0)).body("counters.'drill.connections.rpc.user.unencrypted'.count", greaterThanOrEqualTo(0)).body("counters.'drill.queries.completed'.count", greaterThan(0)).body("histograms.'drill.allocator.huge.hist'.count", greaterThanOrEqualTo(0)).body("histograms.'drill.allocator.normal.hist'.max", greaterThan(0));
   }

    @Test(groups = { "LoggedIn" },enabled = false)
    public void nonAdminAccessThreadsPage() {   //Will fail due to DRILL-6690 (non-Admins can access threads page)
       given().filter(nonAdmin1sessionFilter).when().get("/status/threads").then().statusCode(500).body(containsString("HTTP 403 Forbidden")); 
    }
 
    @Test(groups = { "LoggedIn" })
    public void adminAccessThreadsPage() { 
       given().filter(adminSessionFilter).when().get("/status/threads").then().statusCode(200).body(allOf(containsString("Reference Handler")),containsString("Signal Dispatcher"));
    }

    @Test(groups = { "LoggedIn" })
    public void nonAdminAccessLogsPage() {
       given().filter(nonAdmin1sessionFilter).when().get("/logs.json").then().statusCode(500).body(containsString("HTTP 403 Forbidden"));
    }
 
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "adminRunQuery" })
    public void adminAccessLogsPage() {
       SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
      given().filter(adminSessionFilter).when().get("/logs.json").then().statusCode(200).body("name", hasItems(equalTo("drillbit.log"),equalTo("drillbit.out"),equalTo("drillbit_queries.json"), equalTo("sqlline.log"))).body("lastModified", hasItems(containsString(df.format(new Date()))));
    }

    @Test(groups = { "LoggedIn" },enabled = false)
    public void nonAdminAccessOptionsPage() {  //Will fail due to DRILL-6701 (non-admins can access options page)
       given().filter(nonAdmin1sessionFilter).when().get("/options.json").then().statusCode(500).body(containsString("HTTP 403 Forbidden"));
    }

    @Test(groups = { "LoggedIn" })
    public void adminAccessOptionsPage() {
       given().filter(adminSessionFilter).when().get("/options.json").then().statusCode(200).body("name", hasItems("planner.width.max_per_query","drill.exec.storage.implicit.filename.column.label")).body("value", hasItems(true,false,64,"filename","DEFAULT")).body("accessibleScopes", hasItems("SYSTEM","ALL")).body("kind", hasItems("BOOLEAN","LONG","STRING","DOUBLE"));
    }

    @Parameters({"newPluginFile"})
    @Test(groups = { "LoggedIn" })
    public void nonAdminAddPlugin(String addNewPluginFile) {
       File file = new File(addNewPluginFile);
       given().filter(nonAdmin1sessionFilter).body(file).with().contentType("application/json").when().post("/storage/testPlugin.json").then().statusCode(500).body(containsString("HTTP 403 Forbidden"));
    }

    @Parameters({"newPluginFile"})
    @Test(groups = { "LoggedIn" })
    public void adminAddPlugin(String addNewPluginFile) {
      File file = new File(addNewPluginFile);
      given().filter(adminSessionFilter).body(file).with().contentType("application/json").when().post("/storage/testPlugin.json").then().statusCode(200).body(containsString("success"));   
    }
 
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "adminAddPlugin" })
    public void checkTestPlugin() {
       given().filter(adminSessionFilter).when().get("/storage/testPlugin.json").then().statusCode(200).body("name", equalTo("testPlugin")).body("config.enabled", equalTo(false)).body("config.workspaces.drillTestDirP1.location", equalTo("/drill/testdata/p1tests"));
    }    
    
    @Parameters({"updatePluginFile"})
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "checkTestPlugin" })
    public void adminUpdateTestPlugin(String updateTestPlugin) {
      File file = new File(updateTestPlugin);
      given().filter(adminSessionFilter).body(file).with().contentType("application/json").when().post("/storage/testPlugin.json").then().statusCode(200).body(containsString("success"));
    }

    @Parameters({"queryTestPluginFile"})
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "adminUpdateTestPlugin" })
    public void adminRunQuery(String queryTestPluginFile) {
       File file = new File(queryTestPluginFile);
       given().filter(adminSessionFilter).body(file).with().contentType("application/json").when().post("/query.json").then().statusCode(200).body("columns[0]", equalTo("total_cnt")).body("rows.total_cnt[0]",equalTo("1000"));
    }
    
    @Parameters({"queryTestPluginFile"})
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "adminUpdateTestPlugin" })
    public void nonAdmin1Query(String queryTestPluginFile) {
       File file = new File(queryTestPluginFile);
       given().filter(nonAdmin1sessionFilter).body(file).with().contentType("application/json").when().post("/query.json").then().statusCode(200).body("columns[0]", equalTo("total_cnt")).body("rows.total_cnt[0]",equalTo("1000"));
    }

    @Parameters({"queryTestPluginFile1", "nonAdmin1UserName"})
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "adminRunQuery" })
    public void nonAdmin1ProfilesPage(String queryTestPluginFile1, String nonAdmin1UserName) throws Exception {
       File file = new File(queryTestPluginFile1);
       given().filter(nonAdmin2sessionFilter).body(file).with().contentType("application/json").when().post("/query.json").then().statusCode(200).body("columns[0]", equalTo("total_cnt")).body("rows.total_cnt[0]",equalTo("1000"));
       given().filter(nonAdmin1sessionFilter).when().get("/profiles.json").then().statusCode(200).body("finishedQueries.user", everyItem(equalTo(nonAdmin1UserName)));
    }

    @Parameters({"nonAdmin1UserName","nonAdmin2UserName","adminUserName"})
    @Test(groups = { "LoggedIn" },dependsOnMethods = { "nonAdmin1ProfilesPage" })
    public void adminProfilesPage(String nonAdmin1UserName,String nonAdmin2UserName,String adminUserName) {
       given().filter(adminSessionFilter).when().get("/profiles.json").then().statusCode(200).body("finishedQueries.user", hasItems(nonAdmin1UserName,nonAdmin2UserName,adminUserName)); 
    }

    @Parameters({"cancelRunningQuery1Script","nonAdmin2UserName","nonAdmin2Passwd","query1"})
    @Test(groups = { "LoggedIn" })
    public void nonAdmin1CancelNonAdmin2Query(String cancelRunningQuery1Script,String nonAdmin2UserName,String nonAdmin2Passwd,String sqlQuery) throws Exception {
       File file = new File(cancelRunningQuery1Script);
       //Run the shell script
       Runtime.getRuntime().exec("sh -f " + file + " " + baseHost + ":" + port + " " + nonAdmin2UserName + " " + nonAdmin2Passwd);
       //put 3 seconds sleep time here to make sure query is in running state
       Thread.sleep(3000);
       //Get the response
       Response response = given().filter(adminSessionFilter).get("/profiles.json").then().extract().response();
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
       given().pathParam("queryID", queryId).filter(nonAdmin1sessionFilter).when().get("/profiles/cancel/{queryID}").then().statusCode(200).body(containsString("Failure attempting to cancel query" + " " + queryId));
       //put 5 seconds sleep time
       Thread.sleep(5000);
       //Check to make sure that query is still running or completed and not cancelled or being cancelled
       given().pathParam("queryID", queryId).filter(adminSessionFilter).when().get("/profiles/{queryID}.json").then().statusCode(200).body("state",anyOf(equalTo(1),equalTo(2)));
    }

    @Parameters({"cancelRunningQuery2Script","nonAdmin1UserName","nonAdmin1Passwd","query2"})
    @Test(groups = { "LoggedIn" }, dependsOnMethods = { "nonAdmin1CancelNonAdmin2Query" })
    public void nonAdmin1CancelOwnQuery(String cancelRunningQuery2Script,String nonAdmin1UserName,String nonAdmin1Passwd,String sqlQuery) throws Exception {
       File file = new File(cancelRunningQuery2Script);
       //Run the shell script
       Runtime.getRuntime().exec("sh -f " + file + " " + baseHost + ":" + port + " " + nonAdmin1UserName + " " + nonAdmin1Passwd);
       //put 5 seconds sleep time here to make sure query is in running state
       Thread.sleep(5000);
       //Get the response
       Response response = given().filter(nonAdmin1sessionFilter).get("/profiles.json").then().extract().response();
       //Put the running queries into a list
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
       given().pathParam("queryID", queryId).filter(nonAdmin1sessionFilter).when().get("/profiles/cancel/{queryID}").then().statusCode(200);
       //put 10 seconds sleep time to make sure query is cancelled
       Thread.sleep(10000);
       //Check to make sure that query is cancelled
       given().pathParam("queryID", queryId).filter(nonAdmin1sessionFilter).when().get("/profiles/{queryID}.json").then().statusCode(200).body("state",equalTo(3));
}

    @Parameters({"cancelRunningQuery3Script","nonAdmin2UserName","nonAdmin2Passwd"})
    @Test(groups = { "LoggedIn" }, dependsOnMethods = {"nonAdmin1CancelOwnQuery"})
    public void adminCancelnonAdmin2Query(String cancelRunningQuery3Script,String nonAdmin2UserName,String nonAdmin2Passwd,String sqlQuery) throws Exception {
       File file = new File(cancelRunningQuery3Script);
       //Run the shell script
       Runtime.getRuntime().exec("sh -f " + file + " " + baseHost + ":" + port + " " + nonAdmin2UserName + " " + nonAdmin2Passwd);
       //put 5 seconds sleep time here to make sure query is in running state
       Thread.sleep(5000);
       //Get the response
       Response response = given().filter(nonAdmin2sessionFilter).get("/profiles.json").then().extract().response();
       //Put the running queries into a list
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
       System.out.println("queryId: " + queryId);
       //Cancel the query with the queryId
       given().pathParam("queryID", queryId).filter(adminSessionFilter).when().get("/profiles/cancel/{queryID}").then().statusCode(200);
       //put 10 seconds sleep time to make sure query is cancelled
       Thread.sleep(10000);
       //Check to make sure that query is cancelled
       given().pathParam("queryID", queryId).filter(adminSessionFilter).when().get("/profiles/{queryID}.json").then().statusCode(200).body("state",equalTo(3));
    }

    @Test(groups = { "LoggedIn" },dependsOnMethods = { "adminProfilesPage" })
    public void nonAdminDeletePlugin() {
       given().filter(nonAdmin1sessionFilter).when().delete("/storage/testPlugin.json").then().statusCode(500).body(containsString("HTTP 403 Forbidden"));
    }

    @Test(groups = { "LoggedIn" },dependsOnMethods = { "nonAdminDeletePlugin" })
    public void adminDeletePlugin() {
       given().filter(adminSessionFilter).when().delete("/storage/testPlugin.json").then().statusCode(200).body(containsString("success"));
    }

    @Test(groups = { "LoggedOut" },dependsOnGroups = { "LoggedIn" })
    public void nonAdminLogsOut() {
       given().filter(nonAdmin1sessionFilter).when().get("/logout").then().statusCode(200).body(containsString("Log In"));
    }
}
