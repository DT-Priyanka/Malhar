package com.datatorrent.contrib.solr;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.common.SolrInputDocument;

import com.datatorrent.api.Context.OperatorContext;

/**
 * Default Implementation of AbstractSolrOutputOperator. Accepts maps and puts key values in SolrInputDocument format.
 * Map keys must be added to schema.xml <br>
 * <br>
 * Set solrServerType in properties to instantiate appropriate solr server instance.
 */
public class SolrOutputOperator extends AbstractSolrOutputOperator<Map<String, Object>, SolrServerConnector>
{

  private static final String DEFAULT_SERVER_TYPE = "HttpSolrServer";
  private String solrServerType = DEFAULT_SERVER_TYPE;

  @Override
  public void setup(OperatorContext context)
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

    solrServerConnector = new HttpSolrServerConnector();
    super.setSolrServerConnector(solrServerConnector);
    super.setup(context);
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

  // set this property in dt-site.xml
  public void setSolrServerType(String solrServerType)
  {
    this.solrServerType = solrServerType;
  }

}
