# !/usr/bin/env ruby
# This plugin accesses metrics URIs to gather
# Cassandra cluster metrics, thread pool metrics, 
# operating system metrics, and keyspace column family metrics, 
# and converts the endpoints to a graphite-readable format


require 'sensu-plugin/metric/cli'
require 'net/http'
require 'socket'
require 'json'
require 'thread'

class MetricsTest < Sensu::Plugin::Metric::CLI::Graphite




  METRICS_LIST = [
    'cms-collection-count',
    'cms-collection-time',
    'data-load',
    'heap-committed',
    'heap-max',
    'heap-used',
    'key-cache-hits',
    'key-cache-requests',
    'key-cache-hit-rate',
    'nonheap-committed',
    'nonheap-max',
    'nonheap-used',
    'par-new-collection-count',
    'par-new-collection-time',
    'pending-compaction-tasks',
    'pending-flushes',
    'pending-gossip-stage',
    'pending-hinted-handoff',
    'pending-internal-response-stage',
    'pending-memtable-post-flush',
    'pending-migration-stage',
    'pending-misc-stage',
    'pending-read-stage',
    'pending-read-repair-stage',
    'pending-anti-entropy-stage',
    'pending-repl-on-write-tasks',
    'pending-request-response-stage',
    'pending-mutation-stage',
    # 'pending streams' missing
    'read-latency-op',
    'read-ops',
    'row-cache-hits',
    'row-cache-requests',
    'row-cache-hit-rate',
    'total-compactions-completed',
    'total-bytes-compacted',
    'write-latency-op',
    'write-ops',

    # os metrics
    'os-cpu-idle',
    'os-cpu-iowait',
    'os-cpu-nice',
    'os-cpu-steal',
    'os-cpu-system',
    'os-cpu-user',
    'os-load',
    'os-memory-buffers',
    'os-memory-cached',
    'os-memory-free',
    'os-memory-used',
    'os-net-received'
  ]

  KEYSPACE_METRICS = [
    'cf-keycache-hit-rate',
    'cf-keycache-hits',
    'cf-keycache-requests',
    'cf-live-disk-used',
    'cf-live-sstables',
    'cf-pending-tasks',
    'cf-read-latency-op',
    'cf-read-ops',
    'cf-rowcache-hit-rate',
    'cf-rowcache-hits',
    'cf-rowcache-requests',
    'cf-total-disk-used',
    'cf-write-latency-op',
    'cf-write-ops',
    'cf-bf-space-used',
    'cf-bf-false-positives',
    'cf-bf-false-ratio'
  ]
  
  IGNORE_KEYSPACES = ['OpsCenter', 'system', 'system_auth', 'dse_system', 'dse_perf', 'system_traces']

  option :opscenter_host,
    description: 'Opscenter metrics source host',
    long: '--opscenter-host OPSCENTER-HOST'

  option :opscenter_rest_port,
    description: 'Opscenter REST port, defaults to 8888',
    long: '--opscenter_rest_port OPSCENTER_REST_PORT',
    default: '8888'

  option :cluster_name_param,
    description: 'Limits metrics to a particular cluster',
    long: '--cluster CLUSTER',
    default: 'all'

  option :limit,
    description: 'Limits metrics to a particular type (keyspace or non-keyspace)',
    long: '--limit LIMIT',
    default: 'all'

  option :keyspace,
    description: 'Specifies a keyspace to get metrics for',
    long: '--keyspace KEYSPACE',
    default: 'all'

  option :metrics_list,
    description: 'Specifies non-keyspace metrics to be retrieved',
    long: '--metrics METRICS',
    default:   METRICS_LIST * ','

  option :keyspace_metrics,
    description: 'Specifies keyspace metrics to be retrieved',
    long: '--kmetrics KMETRICS',
    default: KEYSPACE_METRICS * ','

  option :user,
    description: 'opscenter username',
    long: '--user USER'

  option :password,
    description: 'opcenter user password',
    long: '--password PASSWORD'

  option :scheme,
    description: 'Basis for Graphite naming scheme',
    long: '--scheme SCHEME',
    default: 'cassandra'
    
  option :node_list,
    description: 'Specifies nodes to retrieve metrics from',
    long: '--nodes NODES',
    default:  'all'

  option :aggregation,
    description: 'Set to \'1\' to aggregate metrics across all nodes',
    long: '--aggregate AGGREGATE',
    default: '0'

  option :host_format,
    description: 'Replaces dots (.) in nodes hostnames with underscores (_)',
    long: '--host_format HOST_FORMAT',
    default: false

  option :bucket_format,
    description: 'Replaces dots (.) in cluster name with underscores (_)',
    long: '--bucket_format BUCKET_FORMAT',
    default: false

  def dotted *args ; args.join '.' end

  def flattened_metrics obj, ns=[], res={}
    # Flattens metrics JSON to single hash
    case obj
    when Hash
      obj.each do |k,v|
        flattened_metrics v, (ns + [k]), res
      end
      res
    when Array
      obj.each_with_index do |v, i|
        flattened_metrics v, (ns + [i]), res
      end
      res
    when nil
      res
    else
      res[ns] = obj
      res
    end
  end

  def read_metrics obj
    # Converts JSON API output for metrics to simpler format
    res = Hash.new
    obj.each do |node,v|
      data = Hash.new
      v.each do |r|
        metric = r['metric']
        point = r['data-points']
        if point != nil
          data[metric] = point[0][0]
        end
      end
      res[node] = data
    end
    res
  end

  def read_cf_metrics obj
    # Converts JSON API output for keyspace metrics to simpler format
    res = Hash.new
    cf = String.new
    obj.each do |node,v|
      data = Hash.new
      v.each do |r|
        metric = r['metric']
        point = r['data-points']
        cf = r['columnfamily']
        if point != nil
          data[metric] = point[0][0]
        end
      end
      res[node] = {cf => data}
    end
    res
  end

  def get_uri uri, nodes, metrics, aggregate, columns, session, time
    # Accesses API to retrieve metrics
    res = Net::HTTP.start \
      uri.host, uri.port, use_ssl: uri.scheme == 'https' \
    do |http|

      params = {
        'nodes'             => nodes, 
        'node_aggregation'  => aggregate,
        'metrics'           => metrics, 
        'step'               => '60', 
        'start'             => time, 
        'end'               => time,
        'columnfamilies'    => columns
      }
      uri.query = URI.encode_www_form(params)
      req = Net::HTTP::Get.new uri
      req['opscenter-session'] = session

      http.request req
    end

  end

  def get_clusters uri, session
    # Gets data cluster names for metric retrieval
    res = Net::HTTP.start \
      uri.host, uri.port, use_ssl: uri.scheme == 'https' \
    do |http|

      req = Net::HTTP::Get.new uri
      req['opscenter-session'] = session
      cluster_configs = JSON.parse (http.request req).body
      cluster_configs.keys
    end

  end

  def get_nodes uri, session
    # Gets node ips for metric retrieval
    res = Net::HTTP.start \
      uri.host, uri.port, use_ssl: uri.scheme == 'https' \
    do |http|

      req = Net::HTTP::Get.new uri
      req['opscenter-session'] = session
      node_info = JSON.parse (http.request req).body
      node_ips = Array.new
      node_info.each do |node|
        node_ips << node['node_ip']
      end
      node_ips
    end
  end

  def get_datacenters uri, nodes, session
    # Gets datacenter names for node ips
    res = Net::HTTP.start \
      uri.host, uri.port, use_ssl: uri.scheme == 'https' \
    do |http|

      req = Net::HTTP::Get.new uri
      req['opscenter-session'] = session
      node_info = JSON.parse (http.request req).body
      node_datacenters = Hash.new
      node_info.each do |node|
        if nodes.include? node['node_ip']
          node_datacenters[node['node_ip']] = node['dc']
        end
      end
      node_datacenters
    end
  end

  def get_hostnames uri, nodes, session
    # Gets host names from node ips
    res = Net::HTTP.start \
      uri.host, uri.port, use_ssl: uri.scheme == 'https' \
    do |http|

      req = Net::HTTP::Get.new uri
      req['opscenter-session'] = session
      node_info = JSON.parse (http.request req).body
      node_hostnames = Hash.new
      node_info.each do |node|
        if nodes.include? node['node_ip']
          node_hostnames[node['node_ip']] = node['hostname']
        end
      end
      node_hostnames
    end
  end

  def get_column_families uri, keyspace, session
    # Gets column families for keyspace metrics
    res = Net::HTTP.start \
      uri.host, uri.port, use_ssl: uri.scheme == 'https' \
    do |http|

      req = Net::HTTP::Get.new uri
      req['opscenter-session'] = session
      keyspace_info = JSON.parse (http.request req).body
      column_families_output = Array.new
      if keyspace == 'all'
        keyspace_info.each do |k, v|
          if !IGNORE_KEYSPACES.include?(k)
            v['column_families'].keys.each do |cf|
              column_families_output << "#{k}.#{cf}"
            end
          end
        end
      else
        keyspace_info[keyspace]['column_families'].keys.each do |cf|
          column_families_output << "#{keyspace}.#{cf}"
        end
      end
      column_families_output
    end
  end


  def get_session uri
    # Gets session id for API
    login_page = JSON.parse Net::HTTP.post_form(uri, 'username' => config[:user], 'password' => config[:password]).body
    if login_page["sessionid"] == nil
      raise 'Incorrect user or password'
    end
    login_page["sessionid"]
  end


  def run

    # Retrieves most recent available data from data center (5 minutes ago)
    raise 'An opscenter host is required' if config[:opscenter_host].nil?
    raise 'An opscenter user and password are required' if config[:user].nil? || config[:password].nil?
    stamp  = Time.now.to_i - 300
    opscenter_ip = "http://#{config[:opscenter_host]}:#{config[:opscenter_rest_port]}"
    session_id = get_session URI("#{opscenter_ip}/login")

    if config[:focused_cluster] = 'all'
      cluster_names = get_clusters URI("#{opscenter_ip}/cluster-configs"), session_id
    else
      cluster_names = config[:focused_cluster]
    end


    cluster_names.each do |cluster|
      if config[:bucket_format]
        cluster = cluster.gsub '.', '_'
      end

      if config[:node_list] = 'all'
        node_ips = (get_nodes URI("#{opscenter_ip}/#{cluster}/nodes"), session_id) * ','
      else
        node_ips = config[:node_list]
      end

      datacenters = get_datacenters URI("#{opscenter_ip}/#{cluster}/nodes"), node_ips, session_id
      hostnames = get_hostnames URI("#{opscenter_ip}/#{cluster}/nodes"), node_ips, session_id

      if config[:limit] != 'keyspace'
        # Output non-keyspace metrics
        
        metrics_resp  = get_uri URI("#{opscenter_ip}/#{cluster}/new-metrics"), node_ips, config[:metrics_list], config[:aggregation], nil, session_id, stamp
        metrics = JSON.parse metrics_resp.body

        metrics = metrics['data']
        metrics = read_metrics metrics
        metrics = flattened_metrics metrics

        metrics.each do |metric, value|
          datacenter = datacenters[metric[0]]
          hostname = hostnames[metric[0]]
          if config[:host_format]
            hostname = hostname.gsub '.', '_'
          end

          naming_scheme = config[:scheme] + ".#{hostname}"

          metric.delete_at(0)


          metric = dotted naming_scheme, *metric
          output metric, value, stamp
        end
      end


      if config[:limit] != 'non-keyspace'
        # Output keyspace metrics

        column_families = get_column_families(URI("#{opscenter_ip}/#{cluster}/keyspaces"), config[:keyspace], session_id)
        cf_threads = []
        cf_values = []
        column_families.each do |cf|
          cf_threads << Thread.new {
            JSON.parse (get_uri URI("#{opscenter_ip}/#{cluster}/new-metrics"), node_ips, config[:keyspace_metrics], config[:aggregation], cf, session_id, stamp).body
          }
        end
        cf_threads.each {|thr| thr.join}
        cf_threads.each {|thr| cf_values << thr.value}

        cf_values.each do |cf|

          cf_metrics = cf

          cf_metrics = cf_metrics['data']
          cf_metrics = read_cf_metrics cf_metrics
          cf_metrics = flattened_metrics cf_metrics

          cf_metrics.each do |metric, value|
            datacenter = datacenters[metric[0]]
            hostname = hostnames[metric[0]]
            if config[:host_format]
              hostname = hostname.gsub '.', '_'
            end

            naming_scheme = config[:scheme] + ".#{hostname}"

            metric.delete_at(0)

            metric = dotted naming_scheme, *metric
            output metric, value, stamp
          end
        end
      end


      ok
    end
  end



end