package com.datatorrent.contrib.solr;

import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrInputOperator extends AbstractSolrInputOperator<Map<String, Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(SolrInputOperator.class);
  private static final String DEFAULT_SERVER_TYPE = "HttpSolrServer";
  private String solrServerType = DEFAULT_SERVER_TYPE;
  private String solrQuery;

  @Override
  public void initializeSolrServerConnector()
  {
    if ("HttpSolrServer".equals(solrServerType)) {
      solrServerConnector = new HttpSolrServerConnector();
    } else if ("CloudSolrServer".equals(solrServerType)) {
      solrServerConnector = new CloudSolrServerConnector();
    } else if ("ConcurrentUpdateSolrServer".equals(solrServerType)) {
      HttpClient client = HttpClientBuilder.create().build();
      solrServerConnector = new ConcurrentUpdateSolrServerConnector();
      ((ConcurrentUpdateSolrServerConnector) solrServerConnector).setHttpClient(client);
    } else if ("LBHttpSolrServer".equals(solrServerType)) {
      HttpClient client = HttpClientBuilder.create().build();
      solrServerConnector = new LBHttpSolrServerConnector();
      ((LBHttpSolrServerConnector) solrServerConnector).setHttpClient(client);
    }
  }

  @Override
  protected void emitTuple(SolrDocument document)
  {
    outputPort.emit(document.getFieldValueMap());
  }

  @Override
  public SolrParams getQueryParams()
  {
    SolrParams solrParams;
    if (solrQuery != null) {
      solrParams = SolrRequestParsers.parseQueryString(solrQuery);
    } else {
      logger.debug("Solr document fetch query is not set, using wild card query for search.");
      solrParams = SolrRequestParsers.parseQueryString("*");
    }
    return solrParams;
  }

  public String getSolrServerType()
  {
    return solrServerType;
  }

  // set this property in dt-site.xml
  public void setSolrServerType(String solrServerType)
  {
    this.solrServerType = solrServerType;
  }

  public String getSolrQuery()
  {
    return solrQuery;
  }

  // set this property in dt-site.xml
  public void setSolrQuery(String solrQuery)
  {
    this.solrQuery = solrQuery;
  }

}
