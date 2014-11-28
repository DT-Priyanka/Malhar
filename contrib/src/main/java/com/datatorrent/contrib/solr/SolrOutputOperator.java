package com.datatorrent.contrib.solr;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.common.SolrInputDocument;

public class SolrOutputOperator extends AbstractSolrOutputOperator<Map<String, Object>>
{
  private static final String DEFAULT_SERVER_TYPE = "HttpSolrServer";
  private String solrServerType = DEFAULT_SERVER_TYPE;

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
  public SolrInputDocument convertTuple(Map<String, Object> tupleFields)
  {
    SolrInputDocument inputDoc = new SolrInputDocument();
    Iterator<Entry<String, Object>> itr = tupleFields.entrySet().iterator();
    while (itr.hasNext()) {
      Entry<String, Object> field = itr.next();
      inputDoc.setField(field.getKey(), field.getValue());
    }
    return inputDoc;
  }

  public String getSolrServerType()
  {
    return solrServerType;
  }

  public void setSolrServerType(String solrServerType)
  {
    this.solrServerType = solrServerType;
  }

}
