package com.datatorrent.contrib.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;

public class CloudSolrServerConnector extends SolrServerConnector
{

  private String zookeeperHost;
  private boolean updateToLeader;

  @Override
  public void connect() throws IOException
  {
    solrServer = new CloudSolrServer(zookeeperHost, updateToLeader);
  }

  // set this property in dt-site.xml
  public void setSolrZookeeperHost(String solrServerURL)
  {
    this.zookeeperHost = solrServerURL;
  }

  public String getSolrZookeeperHost()
  {
    return zookeeperHost;
  }

  // set this property in dt-site.xml
  public void setUpdateToLeader(boolean updateToLeader)
  {
    this.updateToLeader = updateToLeader;
  }

  public boolean getUpdateToLeader()
  {
    return updateToLeader;
  }

}
