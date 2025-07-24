require 'mysql2'
require 'json'
require 'time'
require 'mail'
require 'fileutils'
require 'yaml'

# --- 1. MySqlMonitorConfig: Handles loading configuration ---
class MySqlMonitorConfig
  attr_reader :db_host, :db_port, :db_name, :db_user, :db_password
  attr_reader :sender_email, :sender_password, :receiver_email, :smtp_address, :smtp_port, :smtp_domain
  attr_reader :iostat_threshold_kb_s, :iostat_device, :cpu_alert_threshold,
              :query_alert_threshold_seconds, :query_kill_threshold_seconds,
              :innodb_buffer_pool_hit_ratio_min, :innodb_dirty_pages_threshold_percent
  attr_reader :alert_cooldown_minutes, :last_alert_file, :last_deadlock_file
  attr_reader :log_file, :log_level
  attr_reader :mysql_log_path, :mysql_error_log_file_pattern, :mysql_slow_log_path, :mysql_slow_log_file_pattern
  attr_reader :auto_kill_rogue_processes
  attr_reader :disk_space_threshold_percent
  attr_reader :replication_lag_seconds_threshold
  attr_reader :connections_warning_percent, :tmp_disk_tables_threshold, :opened_tables_warning_threshold,
              :handler_read_rnd_next_warning_threshold, :handler_read_key_warning_threshold

  def initialize
    config_file = File.expand_path('../config/mysql_monitor_config.yml', __FILE__)
    unless File.exist?(config_file)
      raise "Configuration file not found: #{config_file}. Please ensure it exists in the 'config/' directory."
    end
    config = YAML.load_file(config_file)

    # Database configurations
    @db_host = config['database']['host']
    @db_port = config['database']['port']
    @db_name = config['database']['name']
    @db_user = ENV['MYSQL_USER']
    @db_password = ENV['MYSQL_PASSWORD']
    unless @db_user && @db_password
      raise "Environment variables MYSQL_USER and MYSQL_PASSWORD must be set."
    end

    # Email configurations
    @sender_email = config['email']['sender_email']
    @sender_password = ENV['EMAIL_PASSWORD']
    @receiver_email = config['email']['receiver_email']
    @smtp_address = config['email']['smtp_address']
    @smtp_port = config['email']['smtp_port']
    @smtp_domain = config['email']['smtp_domain']
    unless @sender_password
      raise "Environment variable EMAIL_PASSWORD must be set."
    end

    # Alert thresholds
    @iostat_threshold_kb_s = config['thresholds']['iostat_threshold_kb_s']
    @iostat_device = config['thresholds']['iostat_device']
    @cpu_alert_threshold = config['thresholds']['cpu_threshold_percent']
    @query_alert_threshold_seconds = config['thresholds']['query_alert_threshold_seconds']
    @query_kill_threshold_seconds = config['thresholds']['query_kill_threshold_seconds']
    @innodb_buffer_pool_hit_ratio_min = config['thresholds']['innodb_buffer_pool_hit_ratio_min']
    @innodb_dirty_pages_threshold_percent = config['thresholds']['innodb_dirty_pages_threshold_percent']
    @disk_space_threshold_percent = config['thresholds']['disk_space_threshold_percent']
    @replication_lag_seconds_threshold = config['thresholds']['replication_lag_seconds_threshold']
    @connections_warning_percent = config['thresholds']['connections_warning_percent']
    @tmp_disk_tables_threshold = config['thresholds']['tmp_disk_tables_threshold']
    @opened_tables_warning_threshold = config['thresholds']['opened_tables_warning_threshold']
    @handler_read_rnd_next_warning_threshold = config['thresholds']['handler_read_rnd_next_warning_threshold']
    @handler_read_key_warning_threshold = config['thresholds']['handler_read_key_warning_threshold']

    # Cooldown settings
    @alert_cooldown_minutes = config['cooldown']['alert_cooldown_minutes']
    @last_alert_file = config['cooldown']['last_alert_file']
    @last_deadlock_file = config['cooldown']['last_deadlock_file']
    FileUtils.mkdir_p(File.dirname(@last_alert_file)) # Ensure directory exists
    FileUtils.mkdir_p(File.dirname(@last_deadlock_file)) # Ensure directory exists

    # Logging settings
    @log_file = config['logging']['log_file']
    @log_level = config['logging']['log_level']
    FileUtils.mkdir_p(File.dirname(@log_file)) # Ensure directory exists

    # MySQL logs path
    @mysql_log_path = config['mysql_logs']['path']
    @mysql_error_log_file_pattern = config['mysql_logs']['error_log_file_pattern']
    @mysql_slow_log_path = config['mysql_logs']['slow_log_path']
    @mysql_slow_log_file_pattern = config['mysql_logs']['slow_log_file_pattern']

    # Feature toggles
    @auto_kill_rogue_processes = config['features']['auto_kill_rogue_processes']
  end
end

# --- 2. MySqlConnection: Handles database connection and queries ---
class MySqlConnection
  def initialize(config)
    @config = config
    @client = nil
  end

  def connect
    @client = Mysql2::Client.new(
      host: @config.db_host,
      port: @config.db_port,
      database: @config.db_name,
      username: @config.db_user,
      password: @config.db_password
    )
    @client
  rescue Mysql2::Error => e
    message = "Erro ao conectar ao banco de dados MySQL: #{e.message}"
    raise
  end

  def execute_query(query)
    @client.query(query)
  rescue Mysql2::Error => e
    puts "Erro ao executar consulta MySQL: #{e.message}. Query: #{query.strip[0..100]}..."
    nil
  end

  def close
    @client.close if @client && !@client.closed?
  end

  # Allow direct access to the connection object for specific cases
  def raw_connection
    @client
  end
end

# --- 3. EmailSender: Manages sending emails with cooldown (Same as PostgreSQL script) ---
class EmailSender
  def initialize(config)
    @config = config
    Mail.defaults do
      delivery_method :smtp, {
        address: @config.smtp_address,
        port: @config.smtp_port,
        domain: @config.smtp_domain,
        user_name: @config.sender_email,
        password: @config.sender_password,
        authentication: 'plain',
        enable_starttls_auto: true
      }
    end
  end

  def send_alert_email(subject, body, alert_type = "generic_alert")
    FileUtils.mkdir_p(File.dirname(@config.last_alert_file)) unless File.directory?(File.dirname(@config.last_alert_file))

    last_alert_times = File.exist?(@config.last_alert_file) ? JSON.parse(File.read(@config.last_alert_file)) : {}
    last_sent_time_str = last_alert_times[alert_type]
    last_sent_time = last_sent_time_str ? Time.parse(last_sent_time_str) : nil

    current_local_time = Time.now.strftime('%d/%m/%Y %H:%M:%S')

    if last_sent_time && (Time.now - last_sent_time) < @config.alert_cooldown_minutes * 60
      puts "[#{current_local_time}] Alerta do tipo '#{alert_type}' suprimido devido ao cooldown de #{@config.alert_cooldown_minutes} minutos. Último envio: #{last_sent_time_str}."
      return
    end

    puts "[#{current_local_time}] Enviando e-mail para #{@config.receiver_email} com o assunto: #{subject}"
    Mail.deliver do
      to @config.receiver_email
      from @config.sender_email
      subject subject
      body body
    end
    puts "[#{current_local_time}] E-mail enviado com sucesso."

    last_alert_times[alert_type] = Time.now.iso8601
    File.write(@config.last_alert_file, JSON.pretty_generate(last_alert_times))

  rescue StandardError => e
    puts "[#{current_local_time}] Erro ao enviar e-mail: #{e.message}"
    puts "[#{current_local_time}] Verifique as configurações de SMTP e a senha do aplicativo (se estiver usando Gmail)."
  end
end

# --- Base MetricCollector ---
class MetricCollector
  def initialize(mysql_connection, config, alert_messages)
    @mysql_client = mysql_connection
    @config = config
    @alert_messages = alert_messages # Array to store messages
  end

  def bytes_to_human_readable(bytes)
    return '0 B' if bytes.nil? || bytes == 0
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    i = (Math.log(bytes) / Math.log(1024)).floor
    "#{'%.2f' % (bytes / (1024 ** i))} #{units[i]}"
  end
end

# --- 4. CriticalMetrics: High-frequency monitoring ---
class CriticalMetrics < MetricCollector
  def monitor
    puts "\n--- Monitoramento de Alta Frequência (Crítico) para MySQL ---"

    monitor_mysql_service_status # NEW
    monitor_cpu
    monitor_active_connections
    monitor_long_running_queries
    monitor_active_locks
    monitor_io
    monitor_innodb_buffer_pool_hit_ratio
    monitor_innodb_dirty_pages
    monitor_replication_status_and_lag # Updated to check both
    monitor_deadlocks_from_logs
    monitor_aborted_connections # NEW
  end

  private

  def monitor_mysql_service_status
    puts "\n--- Verificando Status do Serviço MySQL ---"
    begin
      status_output = `systemctl is-active mysql 2>&1`
      if $?.success? && status_output.strip == 'active'
        puts "Serviço MySQL está ATIVO."
      else
        @alert_messages << "ALERTA CRÍTICO: Serviço MySQL NÃO está ativo! Status: #{status_output.strip}. Verifique o serviço."
      end
    rescue Errno::ENOENT
      puts "AVISO: Comando 'systemctl' não encontrado. Verifique se o MySQL está sendo gerenciado por systemd ou ajuste para o gerenciador de serviço correto (ex: 'service mysql status')."
    rescue StandardError => e
      puts "Erro ao verificar status do serviço MySQL: #{e.message}"
      @alert_messages << "ERRO: Falha ao verificar status do serviço MySQL: #{e.message}"
    end
  end

  def monitor_cpu
    begin
      mpstat_output = `mpstat -u ALL 1 1 2>/dev/null`
      if mpstat_output && !mpstat_output.empty?
        lines = mpstat_output.split("\n")
        cpu_line = lines.reverse.find { |line| line.strip.start_with?('Average:') && line.include?('all') }

        if cpu_line
          parts = cpu_line.split(/\s+/)
          idle_percent_index = parts.index('%idle')
          if idle_percent_index
            idle_cpu_percent = parts[idle_percent_index + 1].to_f
            used_cpu_percent = 100.0 - idle_cpu_percent

            if used_cpu_percent > @config.cpu_alert_threshold
              @alert_messages << "ALERTA: Alto uso de CPU detectado! Uso total: #{'%.2f' % used_cpu_percent}% (Limiar: #{@config.cpu_alert_threshold}%)."
              @alert_messages << "  Processos MySQL ativos que podem estar contribuindo para o pico de CPU:"

              active_processes_for_cpu = @mysql_client.execute_query(%Q{
                SELECT id, user, host, db, command, time, state, info
                FROM information_schema.processlist
                WHERE Command != 'Sleep' AND time > 0
                ORDER BY time DESC
                LIMIT 5;
              })
              if active_processes_for_cpu && active_processes_for_cpu.any?
                active_processes_for_cpu.each do |row|
                  @alert_messages << "    - ID: #{row['id']}, Usuário: #{row['user']}, Host: #{row['host']}, DB: #{row['db']}, Comando: #{row['command']}, Tempo: #{row['time']}s, Estado: #{row['state']}, Info: #{row['info'].to_s.strip[0..100]}..."
                end
              else
                @alert_messages << "    Nenhum processo MySQL ativo significativo encontrado durante este período de alto CPU."
              end
            end
          else
            puts "AVISO: Não foi possível parsear a saída do mpstat (%idle). Verifique o formato ou a versão do mpstat."
          end
        else
          puts "AVISO: Linha 'Average: all' não encontrada na saída do mpstat. Verifique o formato do comando ou a saída."
        end
      else
        puts "AVISO: mpstat não retornou dados. Certifique-se de que está instalado e acessível no PATH."
      end
    rescue Errno::ENOENT
      puts "ERRO: Comando 'mpstat' não encontrado. Certifique-se de que o pacote 'sysstat' está instalado."
    rescue StandardError => e
      puts "Erro ao executar mpstat ou processar saída: #{e.message}"
    end
  end

  def monitor_active_connections
    result_conn = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Threads_connected';").first
    result_max_used_conn = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Max_used_connections';").first # NEW
    if result_conn && result_max_used_conn
      total_connections = result_conn['Value'].to_i
      max_used_connections = result_max_used_conn['Value'].to_i # NEW
      result_max = @mysql_client.execute_query("SHOW VARIABLES LIKE 'max_connections';").first
      if result_max
        max_connections = result_max['Value'].to_i
        
        # Current connections near limit
        if total_connections >= max_connections * (@config.connections_warning_percent / 100.0)
          @alert_messages << "ALERTA CRÍTICO: Conexões MySQL (#{total_connections}) estão muito próximas do limite máximo (#{max_connections})! Uso: #{'%.2f' % (total_connections.to_f / max_connections * 100)}%."
        else
          puts "Conexões MySQL atuais: #{total_connections}/#{max_connections} (OK)."
        end

        # Peak connections used
        if max_used_connections >= max_connections * (@config.connections_warning_percent / 100.0)
          @alert_messages << "AVISO: Pico de conexões usadas desde o início do MySQL (#{max_used_connections}) está próximo do limite máximo (#{max_connections}). Considere otimizar conexões ou aumentar 'max_connections'."
        else
          puts "Pico de conexões usadas: #{max_used_connections}/#{max_connections} (OK)."
        end
      end
    end
  end

  def monitor_aborted_connections # NEW
    puts "\n--- Verificando Conexões Abortadas ---"
    aborted_connects = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Aborted_connects';").first
    aborted_clients = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Aborted_clients';").first

    if aborted_connects && aborted_clients
      connects_count = aborted_connects['Value'].to_i
      clients_count = aborted_clients['Value'].to_i

      # Define a simple threshold, or base it on a rate over time if more complex monitoring is needed
      # For now, a non-zero value is an alert, indicating a problem that should be investigated.
      if connects_count > 0 || clients_count > 0
        @alert_messages << "ALERTA: Conexões abortadas detectadas! Aborted_connects: #{connects_count}, Aborted_clients: #{clients_count}. Isso pode indicar problemas de rede, configuração do cliente ou falhas de autenticação. Verifique os logs de erro do MySQL."
      else
        puts "Nenhuma conexão abortada detectada recentemente."
      end
    else
      puts "Não foi possível obter métricas de conexões abortadas."
    end
  end

  def monitor_long_running_queries
    puts "\n--- Verificando e Potencialmente Matando Consultas Excessivamente Longas no MySQL ---"
    long_running_queries = @mysql_client.execute_query(%Q{
      SELECT id, user, host, db, command, time, state, info
      FROM information_schema.processlist
      WHERE Command != 'Sleep'
        AND time > #{@config.query_alert_threshold_seconds}
      ORDER BY time DESC;
    })

    if long_running_queries && long_running_queries.any?
      @alert_messages << "ALERTA CRÍTICO: Consultas MySQL rodando há mais de #{@config.query_alert_threshold_seconds} segundos (e potencialmente encerradas):"
      long_running_queries.each do |row|
        query_info = "  ID: #{row['id']}, Usuário: #{row['user']}, Host: #{row['host']}, DB: #{row['db']}, Comando: #{row['command']}, Tempo: #{row['time']}s, Estado: #{row['state']}, Info: #{row['info'].to_s.strip[0..100]}..."
        @alert_messages << query_info

        if @config.auto_kill_rogue_processes && row['time'].to_i >= @config.query_kill_threshold_seconds
          puts "Tentando MATAR a consulta ID #{row['id']} (duração: #{row['time']}s)..."
          begin
            @mysql_client.execute_query("KILL #{row['id']};")
            @alert_messages << "    ---> SUCESSO: Consulta ID #{row['id']} TERMINADA. <---"
            puts "Consulta ID #{row['id']} terminada com sucesso."
          rescue Mysql2::Error => e
            @alert_messages << "    ---> ERRO: Falha ao MATAR a consulta ID #{row['id']}: #{e.message}. <---"
            puts "Falha ao matar a consulta ID #{row['id']}: #{e.message}."
          end
        else
          puts "Consulta ID #{row['id']} (duração: #{row['time']}s) será apenas alertada, ainda não será encerrada."
        end
      end
    else
      puts "Nenhuma consulta MySQL excessivamente longa encontrada."
    end
  end

  def monitor_active_locks
    puts "\n--- Verificando Bloqueios Ativos Persistentes (InnoDB) ---"
    result_locks = @mysql_client.execute_query(%Q{
      SELECT
          r.trx_id AS waiting_trx_id,
          r.trx_mysql_thread_id AS waiting_thread,
          r.trx_query AS waiting_query,
          b.trx_id AS blocking_trx_id,
          b.trx_mysql_thread_id AS blocking_thread,
          b.trx_query AS blocking_query,
          l.lock_mode,
          l.lock_type,
          l.lock_table,
          l.lock_index,
          w.requesting_engine_lock_id,
          w.blocking_engine_lock_id,
          (SELECT PROCESSLIST_TIME FROM performance_schema.threads WHERE PROCESSLIST_ID = r.trx_mysql_thread_id) AS waiting_duration_seconds
      FROM information_schema.innodb_lock_waits w
      INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
      INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
      INNER JOIN information_schema.innodb_locks l ON l.lock_id = w.requested_lock_id
      WHERE (SELECT PROCESSLIST_TIME FROM performance_schema.threads WHERE PROCESSLIST_ID = r.trx_mysql_thread_id) > 30; -- Only show locks waiting for more than 30 seconds
    })

    if result_locks && result_locks.any?
      @alert_messages << "ALERTA CRÍTICO: Bloqueios Ativos Persistentes Detectados (espera >= 30s):"
      result_locks.each do |row|
        @alert_messages << "  Transação Bloqueada (ID: #{row['waiting_thread']}, Query: #{row['waiting_query'].to_s.strip[0..100]}...)"
        @alert_messages << "  Transação Bloqueadora (ID: #{row['blocking_thread']}, Query: #{row['blocking_query'].to_s.strip[0..100]}...)"
        @alert_messages << "  Bloqueio: Modo: #{row['lock_mode']}, Tipo: #{row['lock_type']}, Tabela: #{row['lock_table'] || 'N/A'}, Índice: #{row['lock_index'] || 'N/A'}"
        @alert_messages << "  Duração da Espera: #{row['waiting_duration_seconds']} segundos"
        @alert_messages << "  ---"
      end
    else
      puts "Nenhum bloqueio persistente detectado."
    end
  rescue Mysql2::Error => e
    if e.message.include?('Unknown table') || e.message.include?('Access denied')
      puts "AVISO: Falha ao consultar INNODB_LOCK_WAITS/performance_schema.threads. Verifique se performance_schema está habilitado e as permissões do usuário. Detalhes: #{e.message}"
    else
      puts "Erro ao verificar bloqueios: #{e.message}"
    end
    @alert_messages << "ERRO: Falha ao verificar bloqueios MySQL: #{e.message}"
  end


  def monitor_io
    begin
      iostat_output = `iostat -k #{@config.iostat_device} 1 2 2>/dev/null`
      if iostat_output && !iostat_output.empty?
        lines = iostat_output.split("\n")
        disk_line = lines.reverse.find { |line| line.strip.start_with?(@config.iostat_device) }

        if disk_line
          parts = disk_line.split(/\s+/)
          rkbs_idx = parts.index(@config.iostat_device) + 3 if parts.index(@config.iostat_device)
          wkbs_idx = parts.index(@config.iostat_device) + 4 if parts.index(@config.iostat_device)

          rkbs = (rkbs_idx && parts[rkbs_idx]) ? parts[rkbs_idx].to_f : 0.0
          wkbs = (wkbs_idx && parts[wkbs_idx]) ? parts[wkbs_idx].to_f : 0.0

          total_kb_s = rkbs + wkbs

          if total_kb_s > @config.iostat_threshold_kb_s
            @alert_messages << "ALERTA: Alto I/O de disco em '#{@config.iostat_device}' detectado! Total: #{'%.2f' % (total_kb_s / 1024)} MB/s (Leitura: #{'%.2f' % (rkbs / 1024)} MB/s, Escrita: #{'%.2f' % (wkbs / 1024)} MB/s)."
            @alert_messages << "  Processos MySQL ativos durante o pico de I/O:"

            active_processes_for_io = @mysql_client.execute_query(%Q{
              SELECT id, user, host, db, command, time, state, info
              FROM information_schema.processlist
              WHERE Command != 'Sleep' AND time > 0
              ORDER BY time ASC
              LIMIT 5;
            })
            if active_processes_for_io && active_processes_for_io.any?
              active_processes_for_io.each do |row|
                @alert_messages << "    - ID: #{row['id']}, Usuário: #{row['user']}, Host: #{row['host']}, DB: #{row['db']}, Comando: #{row['command']}, Tempo: #{row['time']}s, Estado: #{row['state']}, Info: #{row['info'].to_s.strip[0..100]}..."
              end
            else
              @alert_messages << "    Nenhum processo MySQL ativo significativo encontrado durante este período de alto I/O."
            end
          end
        else
          puts "AVISO: Dispositivo '#{@config.iostat_device}' não encontrado na saída do iostat. Verifique o nome do dispositivo ou se o iostat está funcionando."
        end
      else
        puts "AVISO: iostat não retornou dados. Certifique-se de que está instalado e acessível no PATH."
      end
    rescue Errno::ENOENT
      puts "ERRO: Comando 'iostat' não encontrado. Certifique-se de que o pacote 'sysstat' está instalado."
    rescue StandardError => e
      puts "Erro ao executar iostat ou processar saída: #{e.message}"
    end
  end

  def monitor_innodb_buffer_pool_hit_ratio
    puts "\n--- Verificando Innodb Buffer Pool Hit Ratio ---"
    innodb_status = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool%';").to_a.map { |r| [r['Variable_name'], r['Value'].to_i] }.to_h

    if innodb_status['Innodb_buffer_pool_read_requests'] && innodb_status['Innodb_buffer_pool_reads']
      read_requests = innodb_status['Innodb_buffer_pool_read_requests']
      disk_reads = innodb_status['Innodb_buffer_pool_reads']

      if read_requests > 0
        hit_ratio = ((read_requests - disk_reads) / read_requests.to_f) * 100
        if hit_ratio < @config.innodb_buffer_pool_hit_ratio_min
          @alert_messages << "AVISO: Innodb Buffer Pool Hit Ratio baixo (#{('%.2f' % hit_ratio)}%). Considere ajustar innodb_buffer_pool_size ou otimizar consultas."
        else
          puts "Innodb Buffer Pool Hit Ratio: #{('%.2f' % hit_ratio)}% (OK)."
        end
      else
        puts "Innodb Buffer Pool sem requests de leitura ainda."
      end
    else
      puts "Não foi possível obter métricas do Innodb Buffer Pool."
      @alert_messages << "ERRO: Falha ao obter métricas do Innodb Buffer Pool."
    end
  end

  def monitor_innodb_dirty_pages
    puts "\n--- Verificando Porcentagem de Páginas Sujas no InnoDB Buffer Pool ---"
    innodb_status = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool%';").to_a.map { |r| [r['Variable_name'], r['Value'].to_i] }.to_h

    if innodb_status['Innodb_buffer_pool_pages_total'] && innodb_status['Innodb_buffer_pool_pages_dirty']
      total_pages = innodb_status['Innodb_buffer_pool_pages_total']
      dirty_pages = innodb_status['Innodb_buffer_pool_pages_dirty']

      if total_pages > 0
        dirty_percent = (dirty_pages.to_f / total_pages) * 100
        if dirty_percent > @config.innodb_dirty_pages_threshold_percent
          @alert_messages << "ALERTA: Porcentagem de páginas sujas no Innodb Buffer Pool alta (#{('%.2f' % dirty_percent)}%). (Limiar: #{@config.innodb_dirty_pages_threshold_percent}%). Isso pode indicar gargalo de I/O de escrita. Considere ajustar innodb_max_dirty_pages_pct, innodb_io_capacity."
        else
          puts "Páginas sujas no Innodb Buffer Pool: #{('%.2f' % dirty_percent)}% (OK)."
        end
      else
        puts "Innodb Buffer Pool sem páginas ainda."
      end
    else
      puts "Não foi possível obter métricas de páginas sujas do Innodb Buffer Pool."
      @alert_messages << "ERRO: Falha ao obter métricas de páginas sujas do Innodb Buffer Pool."
    end
  end

  def monitor_replication_status_and_lag # Renamed to reflect combined check
    puts "\n--- Verificando Lag e Status de Replicação MySQL (Slave Status) ---"
    begin
      result_replication = @mysql_client.execute_query("SHOW SLAVE STATUS;").first

      if result_replication
        slave_io_running = result_replication['Slave_IO_Running']
        slave_sql_running = result_replication['Slave_SQL_Running']
        seconds_behind_master = result_replication['Seconds_Behind_Master']
        last_io_error = result_replication['Last_IO_Error']
        last_sql_error = result_replication['Last_SQL_Error']

        replication_healthy = true

        if slave_io_running != 'Yes'
          @alert_messages << "ALERTA CRÍTICO: Replicação MySQL IO Thread NÃO está rodando! Slave_IO_Running: #{slave_io_running}. Último Erro: #{last_io_error || 'Nenhum erro reportado'}"
          replication_healthy = false
        end
        if slave_sql_running != 'Yes'
          @alert_messages << "ALERTA CRÍTICO: Replicação MySQL SQL Thread NÃO está rodando! Slave_SQL_Running: #{slave_sql_running}. Último Erro: #{last_sql_error || 'Nenhum erro reportado'}"
          replication_healthy = false
        end

        if last_io_error && !last_io_error.empty?
          @alert_messages << "ALERTA: Erro no Slave IO Thread: #{last_io_error}"
          replication_healthy = false
        end
        if last_sql_error && !last_sql_error.empty?
          @alert_messages << "ALERTA: Erro no Slave SQL Thread: #{last_sql_error}"
          replication_healthy = false
        end

        if seconds_behind_master && seconds_behind_master.to_i > @config.replication_lag_seconds_threshold
          @alert_messages << "ALERTA: Lag de replicação MySQL significativo! #{seconds_behind_master} segundos atrás do master (Limiar: #{@config.replication_lag_seconds_threshold}s)."
          @alert_messages << "  Master Host: #{result_replication['Master_Host']}, Master Port: #{result_replication['Master_Port']}"
          replication_healthy = false
        end

        if replication_healthy
          puts "Replicação MySQL está ok. Lag: #{seconds_behind_master || 'N/A'} segundos."
        end
      else
        puts "Nenhuma informação de slave status encontrada (ou esta é uma instância master/standalone)."
      end
    rescue Mysql2::Error => e
      puts "Erro ao verificar lag e status de replicação MySQL: #{e.message}"
      @alert_messages << "ERRO: Falha ao verificar lag e status de replicação MySQL: #{e.message}"
    end
  end

  def monitor_deadlocks_from_logs
    puts "\n--- Verificando Deadlocks Recentes nos Logs de Erro do MySQL ---"
    if @config.mysql_log_path && File.directory?(@config.mysql_log_path)
      error_log_files = Dir.glob(File.join(@config.mysql_log_path, @config.mysql_error_log_file_pattern)).sort_by { |f| File.mtime(f) }.last(2) # Check last 2 log files

      deadlock_log_entries = []
      deadlock_count_current_run = 0

      # Load last known count
      last_deadlock_data = File.exist?(@config.last_deadlock_file) ? JSON.parse(File.read(@config.last_deadlock_file)) : {}
      last_known_deadlock_time_str = last_deadlock_data['last_deadlock_timestamp']
      last_known_deadlock_time = last_known_deadlock_time_str ? Time.parse(last_known_deadlock_time_str) : Time.now - (3600 * 24 * 7) # default to 7 days ago if never checked

      error_log_files.each do |log_file|
        File.foreach(log_file) do |line|
          # MySQL deadlock messages often contain "DEADLOCK" and transaction details
          if line.include?('DEADLOCK') && line.include?('TRANSACTION')
            begin
              # Attempt to parse timestamp from MySQL log line
              # Example: 2023-01-01T10:00:00.123456Z
              log_time_str_match = line.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{6})?Z)/)
              log_time = log_time_str_match ? Time.parse(log_time_str_match[1]) : nil

              if log_time && log_time > last_known_deadlock_time
                deadlock_log_entries << line.strip
                deadlock_count_current_run += 1
                last_known_deadlock_time = log_time if log_time > last_known_deadlock_time # Update to latest deadlock time
              end
            rescue ArgumentError => e
              puts "AVISO: Não foi possível parsear a data/hora da linha de log para deadlock: #{line.strip} - #{e.message}"
              if File.mtime(log_file) > Time.now - 3600 # Fallback: check if the file itself was modified recently (last hour)
                 deadlock_log_entries << line.strip unless deadlock_log_entries.include?(line.strip) # Add if not already added
                 deadlock_count_current_run += 1
              end
            end
          end
        end
      end

      if deadlock_log_entries.any?
        @alert_messages << "ALERTA: #{deadlock_count_current_run} novo(s) deadlock(s) detectado(s) desde o último check! Verifique os logs do MySQL para mais detalhes."
        @alert_messages << "  Últimos Deadlocks nos logs:"
        deadlock_log_entries.each { |entry| @alert_messages << "    - #{entry}" }
      else
        puts "Nenhum novo deadlock detectado nos logs."
      end

      # Save current latest deadlock timestamp for next run
      File.write(@config.last_deadlock_file, JSON.pretty_generate({'last_deadlock_timestamp' => last_known_deadlock_time.iso8601}))
    else
      puts "AVISO: Caminho de log de erro do MySQL não configurado ou não encontrado. Não foi possível verificar deadlocks nos logs."
      @alert_messages << "AVISO: Caminho de log de erro do MySQL não configurado, não foi possível verificar deadlocks."
    end
  rescue StandardError => e
    puts "Erro ao verificar deadlocks nos logs do MySQL: #{e.message}"
    @alert_messages << "ERRO: Falha ao verificar deadlocks nos logs do MySQL: #{e.message}"
  end
end

# --- 5. PerformanceMetrics: Medium-frequency monitoring ---
class PerformanceMetrics < MetricCollector
  def monitor
    puts "\n--- Monitoramento de Média Frequência (Desempenho) para MySQL ---"
    monitor_table_fragmentation
    monitor_innodb_row_lock_waits
    monitor_tmp_disk_tables # NEW
    monitor_opened_tables_cache # NEW
    monitor_handler_operations # NEW
    monitor_network_throughput # NEW
  end

  private

  def monitor_table_fragmentation
    puts "\n--- Verificando Fragmentação de Tabelas (InnoDB) ---"
    result_frag = @mysql_client.execute_query(%Q{
      SELECT
          table_schema,
          table_name,
          data_length,
          data_free,
          (data_free / data_length) * 100 AS fragmentation_percent
      FROM information_schema.tables
      WHERE table_schema = DATABASE() AND engine = 'InnoDB'
      AND data_length > 0 AND (data_free / data_length) * 100 > 10 -- more than 10% fragmentation
      ORDER BY fragmentation_percent DESC
      LIMIT 10;
    })

    if result_frag && result_frag.any?
      @alert_messages << "AVISO: Tabelas InnoDB com fragmentação significativa (>10%) (Top 10):"
      result_frag.each do |row|
        @alert_messages << "  - Tabela: #{row['table_schema']}.#{row['table_name']}"
        @alert_messages << "    Tamanho Dados: #{bytes_to_human_readable(row['data_length'])}, Espaço Livre: #{bytes_to_human_readable(row['data_free'])}"
        @alert_messages << "    Fragmentação: #{'%.2f' % row['fragmentation_percent']}%"
        @alert_messages << "    Ação: Considere `OPTIMIZE TABLE #{row['table_schema']}.#{row['table_name']};` (requer downtime ou ferramenta online)."
      end
    else
      puts "Nenhuma tabela InnoDB com fragmentação significativa detectada."
    end
  end

  def monitor_innodb_row_lock_waits
    puts "\n--- Verificando Esperas por Bloqueio de Linha InnoDB ---"
    result_lock_waits = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Innodb_row_lock%';").to_a.map { |r| [r['Variable_name'], r['Value'].to_i] }.to_h

    if result_lock_waits['Innodb_row_lock_waits'] && result_lock_waits['Innodb_row_lock_time']
      total_waits = result_lock_waits['Innodb_row_lock_waits']
      total_wait_time_ms = result_lock_waits['Innodb_row_lock_time']

      if total_waits > 0
        avg_wait_time_ms = total_wait_time_ms.to_f / total_waits
        @alert_messages << "AVISO: Foram detectadas esperas por bloqueio de linha InnoDB. Total de esperas: #{total_waits}, Tempo total de espera: #{total_wait_time_ms}ms, Tempo médio por espera: #{'%.2f' % avg_wait_time_ms}ms."
        @alert_messages << "  Isso pode indicar contenção em linhas. Verifique consultas e transações."
      else
        puts "Nenhuma espera por bloqueio de linha InnoDB detectada recentemente."
      end
    else
      puts "Não foi possível obter métricas de bloqueio de linha InnoDB."
      @alert_messages << "ERRO: Falha ao obter métricas de bloqueio de linha InnoDB."
    end
  end

  def monitor_tmp_disk_tables # NEW
    puts "\n--- Verificando Tabelas Temporárias Criadas em Disco ---"
    result_tmp_tables = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Created_tmp_tables';").first
    result_tmp_disk_tables = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Created_tmp_disk_tables';").first

    if result_tmp_tables && result_tmp_disk_tables
      total_tmp_tables = result_tmp_tables['Value'].to_i
      tmp_disk_tables = result_tmp_disk_tables['Value'].to_i

      if total_tmp_tables > 0
        disk_ratio = (tmp_disk_tables.to_f / total_tmp_tables) * 100
        if disk_ratio > @config.tmp_disk_tables_threshold
          @alert_messages << "AVISO: Alta porcentagem de tabelas temporárias criadas em disco (#{('%.2f' % disk_ratio)}%). (Limiar: #{@config.tmp_disk_tables_threshold}%). Total: #{total_tmp_tables}, Em disco: #{tmp_disk_tables}. Considere aumentar `tmp_table_size` e `max_heap_table_size`."
        else
          puts "Tabelas temporárias em disco: #{('%.2f' % disk_ratio)}% (OK)."
        end
      else
        puts "Nenhuma tabela temporária criada ainda."
      end
    else
      puts "Não foi possível obter métricas de tabelas temporárias."
    end
  end

  def monitor_opened_tables_cache # NEW
    puts "\n--- Verificando Cache de Tabelas Abertas ---"
    result_opened_tables = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Opened_tables';").first
    result_open_tables = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Open_tables';").first
    result_table_open_cache = @mysql_client.execute_query("SHOW VARIABLES LIKE 'table_open_cache';").first

    if result_opened_tables && result_open_tables && result_table_open_cache
      opened_tables = result_opened_tables['Value'].to_i # Number of tables that have been opened
      open_tables = result_open_tables['Value'].to_i # Number of tables currently open
      table_open_cache_size = result_table_open_cache['Value'].to_i

      # Calculate efficiency as a simple ratio
      # If opened_tables (ever opened) is much higher than table_open_cache_size, it might indicate misses.
      # Or, more directly, compare `Open_tables` against `table_open_cache` limit.
      # A simple check: if `Open_tables` is near `table_open_cache_size` and `Opened_tables` is high
      if open_tables >= table_open_cache_size * (@config.opened_tables_warning_threshold / 100.0) && opened_tables > table_open_cache_size
        @alert_messages << "AVISO: Cache de tabelas abertas pode estar sobrecarregado. Tabelas abertas atualmente: #{open_tables} (limite: #{table_open_cache_size}). Já abriu #{opened_tables} tabelas no total. Considere aumentar `table_open_cache`."
      else
        puts "Cache de tabelas abertas: #{open_tables}/#{table_open_cache_size} (OK)."
      end
    else
      puts "Não foi possível obter métricas de cache de tabelas abertas."
    end
  end

  def monitor_handler_operations # NEW
    puts "\n--- Verificando Operações do Handler (Leituras de Linhas) ---"
    handler_status = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Handler_read%';").to_a.map { |r| [r['Variable_name'], r['Value'].to_i] }.to_h

    handler_read_rnd_next = handler_status['Handler_read_rnd_next'] || 0 # Full table scans
    handler_read_key = handler_status['Handler_read_key'] || 0 # Index reads

    # A very simple check: If Handler_read_rnd_next is high relative to Handler_read_key, or exceeds a threshold
    # This might need more sophisticated analysis based on query patterns.
    # For now, if Handler_read_rnd_next is above a certain absolute threshold, and Handler_read_key is relatively low
    if handler_read_rnd_next > @config.handler_read_rnd_next_warning_threshold
      @alert_messages << "AVISO: Alto número de varreduras completas de tabelas detectado (Handler_read_rnd_next: #{handler_read_rnd_next}). Isso pode indicar falta de índices. Último Handler_read_key: #{handler_read_key}."
    else
      puts "Operações do Handler (Handler_read_rnd_next: #{handler_read_rnd_next}, Handler_read_key: #{handler_read_key}) estão ok."
    end
  rescue StandardError => e
    puts "Erro ao verificar operações do Handler: #{e.message}"
    @alert_messages << "ERRO: Falha ao verificar operações do Handler: #{e.message}"
  end

  def monitor_network_throughput # NEW
    puts "\n--- Verificando Throughput de Rede do MySQL ---"
    bytes_received = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Bytes_received';").first
    bytes_sent = @mysql_client.execute_query("SHOW GLOBAL STATUS LIKE 'Bytes_sent';").first

    if bytes_received && bytes_sent
      total_bytes_received = bytes_received['Value'].to_i
      total_bytes_sent = bytes_sent['Value'].to_i

      # These are cumulative counters. To get rate, you need to store previous values and calculate difference over time.
      # For a simple check, just report the cumulative values. Alerting on rate needs more state.
      # For now, just report for informational purposes in low frequency, or alert on extremely high values.
      @alert_messages << "INFO: MySQL throughput de rede: Recebido: #{bytes_to_human_readable(total_bytes_received)}, Enviado: #{bytes_to_human_readable(total_bytes_sent)}."
    else
      puts "Não foi possível obter métricas de throughput de rede."
    end
  end
end

# --- 6. OptimizationMetrics: Low-frequency monitoring ---
class OptimizationMetrics < MetricCollector
  def monitor
    puts "\n--- Monitoramento de Baixa Frequência (Otimização/Tendências) para MySQL ---"
    monitor_disk_space
    monitor_disk_space_partition
    monitor_unused_indexes
    monitor_slow_queries_from_log # Using slow query log
    analyze_table_sizes # For general table growth
  end

  private

  def monitor_disk_space
    result_disk = @mysql_client.execute_query("SELECT SUM(data_length + index_length) AS total_size FROM information_schema.tables WHERE table_schema = '#{@config.db_name}';").first
    if result_disk && result_disk['total_size']
      db_size = bytes_to_human_readable(result_disk['total_size'].to_i)
      puts "Tamanho do banco de dados '#{@config.db_name}': #{db_size}"
      @alert_messages << "INFO: Tamanho total do banco de dados '#{@config.db_name}': #{db_size}."
    else
      puts "Não foi possível obter o tamanho do banco de dados '#{@config.db_name}'."
      @alert_messages << "ERRO: Falha ao obter o tamanho do banco de dados MySQL."
    end
  end

  def monitor_disk_space_partition
    puts "\n--- Verificando Espaço em Disco da Partição de Dados do MySQL ---"
    datadir_result = @mysql_client.execute_query("SHOW VARIABLES LIKE 'datadir';").first
    if datadir_result && datadir_result['Value']
      data_directory = datadir_result['Value']

      begin
        df_output = `df -h #{data_directory} 2>/dev/null`
        lines = df_output.split("\n")

        disk_info_line = lines.find { |line| line.start_with?('/') || line.start_with?('Filesystem') }

        if disk_info_line && !disk_info_line.include?('Filesystem')
          parts = disk_info_line.split(/\s+/)

          use_percent_str = parts.find { |p| p.end_with?('%') }

          if use_percent_str
            used_percent = use_percent_str.chomp('%').to_i
            mounted_on = parts.last

            if used_percent >= @config.disk_space_threshold_percent
              @alert_messages << "ALERTA CRÍTICO: A partição de dados do MySQL (#{mounted_on}) está com #{used_percent}% de uso, próximo do limite! (Limiar: #{@config.disk_space_threshold_percent}%)"
              @alert_messages << "  Caminho do diretório de dados: #{data_directory}"
            else
              puts "Espaço em disco na partição de dados (#{mounted_on}) está ok: #{used_percent}% de uso."
            end
          else
            puts "AVISO: Não foi possível parsear a porcentagem de uso na saída do 'df' para '#{data_directory}'."
          end
        else
          puts "AVISO: Informações de disco para '#{data_directory}' não encontradas na saída do 'df'."
        end
      rescue Errno::ENOENT
        @alert_messages << "ERRO: Comando 'df' não encontrado. Verifique se está disponível no PATH do sistema."
      rescue StandardError => e
        @alert_messages << "ERRO: Falha ao verificar espaço em disco da partição de dados: #{e.message}"
      end
    else
      puts "Não foi possível obter o diretório de dados do MySQL."
      @alert_messages << "ERRO: Falha ao obter o diretório de dados do MySQL para verificar espaço em disco."
    end
  end

  def monitor_unused_indexes
    puts "\n--- Verificando Índices Não Utilizados (Information Schema e Performance Schema) ---"
    result_unused_idx = @mysql_client.execute_query(%Q{
      SELECT
          s.SCHEMA_NAME,
          s.OBJECT_NAME AS TABLE_NAME,
          s.INDEX_NAME,
          t.INDEX_LENGTH
      FROM performance_schema.table_io_waits_summary_by_index_usage s
      JOIN information_schema.tables t
          ON s.SCHEMA_NAME = t.TABLE_SCHEMA
          AND s.OBJECT_NAME = t.TABLE_NAME
      WHERE s.INDEX_NAME IS NOT NULL
      AND s.COUNT_STAR = 0 -- Index not used
      AND t.TABLE_SCHEMA = DATABASE() -- Only current database
      AND t.INDEX_LENGTH > (1024 * 1024 * 1024) -- Indexes larger than 1GB (arbitrary threshold)
      ORDER BY t.INDEX_LENGTH DESC
      LIMIT 10;
    })

    if result_unused_idx && result_unused_idx.any?
      @alert_messages << "INFORMAÇÃO: Top 10 Índices Não Utilizados (maiores que 1GB, baseados em performance_schema):"
      result_unused_idx.each do |row|
        @alert_messages << "  - Tabela: #{row['TABLE_NAME']}, Índice: #{row['INDEX_NAME']}, Tamanho: #{bytes_to_human_readable(row['INDEX_LENGTH'].to_i)}. Considere remover."
      end
    else
      puts "Nenhum índice grande e não utilizado detectado via performance_schema."
    end
  rescue Mysql2::Error => e
    if e.message.include?('Unknown table') || e.message.include?('Access denied')
      puts "AVISO: Falha ao consultar performance_schema.table_io_waits_summary_by_index_usage. Verifique se performance_schema está habilitado e as permissões do usuário. Detalhes: #{e.message}"
    else
      puts "Erro ao verificar índices não utilizados: #{e.message}"
    end
    @alert_messages << "ERRO: Falha ao verificar índices não utilizados: #{e.message}"
  end

  def monitor_slow_queries_from_log
    puts "\n--- Verificando Slow Query Log do MySQL ---"
    if @config.mysql_slow_log_path && File.directory?(@config.mysql_slow_log_path)
      slow_log_files = Dir.glob(File.join(@config.mysql_slow_log_path, @config.mysql_slow_log_file_pattern)).sort_by { |f| File.mtime(f) }.last(2) # Check last 2 log files

      slow_queries_found = []
      query_time_regex = /Query_time: (\d+\.\d+)\s+Lock_time: (\d+\.\d+).*?\n(?:SET timestamp=.*?;)?\n(.*?)(?:# User@Host:|\Z)/m # Adjusted regex to capture query more reliably

      slow_log_files.each do |log_file|
        log_content = File.read(log_file)
        log_content.scan(query_time_regex) do |match|
          query_time = match[0].to_f
          lock_time = match[1].to_f
          query = match[2].strip

          if query_time > @config.query_alert_threshold_seconds
            slow_queries_found << {
              query_time: query_time,
              lock_time: lock_time,
              query: query
            }
          end
        end
      end

      if slow_queries_found.any?
        top_slow_queries = slow_queries_found.sort_by { |q| -q[:query_time] }.first(10)
        @alert_messages << "ALERTA: Top 10 Consultas Lentas encontradas no Slow Query Log (Tempo de Query > #{@config.query_alert_threshold_seconds}s):"
        top_slow_queries.each do |q|
          @alert_messages << "  - Tempo: #{'%.2f' % q[:query_time]}s (Lock: #{'%.2f' % q[:lock_time]}s), Query: #{q[:query][0..100]}..."
        end
        @alert_messages << "  Verifique o log de consultas lentas em: #{@config.mysql_slow_log_path}"
      else
        puts "Nenhuma consulta lenta significativa encontrada no Slow Query Log."
      end
    else
      puts "AVISO: Caminho do Slow Query Log do MySQL não configurado ou não encontrado. Não foi possível verificar consultas lentas."
      @alert_messages << "AVISO: Caminho do Slow Query Log do MySQL não configurado, não foi possível verificar consultas lentas."
    end
  rescue StandardError => e
    puts "Erro ao verificar Slow Query Log: #{e.message}"
    @alert_messages << "ERRO: Falha ao verificar Slow Query Log: #{e.message}"
  end

  def analyze_table_sizes
    puts "\n--- Análise de Crescimento de Tabelas (Top 10 Maiores) ---"
    result_table_sizes = @mysql_client.execute_query(%Q{
      SELECT
          table_schema,
          table_name,
          data_length + index_length AS total_size,
          data_length,
          index_length
      FROM information_schema.tables
      WHERE table_schema = DATABASE() AND engine = 'InnoDB'
      ORDER BY total_size DESC
      LIMIT 10;
    })

    if result_table_sizes && result_table_sizes.any?
      @alert_messages << "INFORMAÇÃO: Top 10 Maiores Tabelas no Banco de Dados '#{@config.db_name}':"
      result_table_sizes.each do |row|
        @alert_messages << "  - Tabela: #{row['table_schema']}.#{row['table_name']}"
        @alert_messages << "    Tamanho Total: #{bytes_to_human_readable(row['total_size'].to_i)}"
        @alert_messages << "    Tamanho Dados: #{bytes_to_human_readable(row['data_length'].to_i)}"
        @alert_messages << "    Tamanho Índices: #{bytes_to_human_readable(row['index_length'].to_i)}"
      end
    else
      puts "Nenhuma tabela InnoDB encontrada para análise de tamanho."
    end
  end
end

# --- 7. HealthChecker: Data integrity tests ---
class HealthChecker < MetricCollector
  def check
    puts "\n--- TESTES DE SAÚDE E INTEGRIDADE DE DADOS DO MYSQL ---"
    check_table_integrity
    run_custom_sanity_checks
  end

  private

  def check_table_integrity
    puts "Executando CHECK TABLE para tabelas críticas (apenas para MyISAM ou InnoDB com validação leve)..."
    tables_to_check = @mysql_client.execute_query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();").map { |r| r['table_name'] }

    tables_to_check.each do |table_name|
      begin
        result_check = @mysql_client.execute_query("CHECK TABLE `#{table_name}`;").first
        if result_check['Msg_type'] == 'status' && result_check['Msg_text'] == 'OK'
          # puts "  - Tabela '#{table_name}': OK."
        else
          @alert_messages << "ALERTA CRÍTICO: Integridade da tabela '#{table_name}' comprometida! Status: #{result_check['Msg_text']}. Verifique o log de erro do MySQL."
        end
      rescue Mysql2::Error => e
        @alert_messages << "ERRO: Falha ao executar CHECK TABLE para '#{table_name}': #{e.message}"
      end
    end
    puts "Verificação de integridade de tabelas concluída."
  end

  def run_custom_sanity_checks
    puts "\n--- 2. Consultas de Sanidade Customizadas (ADAPTE PARA SUA ESTRUTURA!) ---"
    result_count = @mysql_client.execute_query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE();").first
    if result_count
      count = result_count.values.first.to_i
      puts "Contagem de tabelas no banco '#{@config.db_name}': #{count}"
      if count < 5
        @alert_messages << "ALERTA: Número de tabelas no banco '#{@config.db_name}' inesperadamente baixo (#{count}). Verifique a integridade do schema."
      else
        puts "Contagem de tabelas está dentro do esperado."
      end
    else
      puts "Erro ao executar consulta de contagem em information_schema.tables."
      @alert_messages << "ERRO: Falha ao executar consulta de sanidade: Contagem de tabelas."
    end
    puts "\n--- Lembre-se de ADAPTAR as consultas de sanidade para seu ambiente! ---"
  end
end

# --- 8. TableSizeHistory: Saves table size historical data ---
class TableSizeHistory < MetricCollector
  def save
    puts "\n--- Salvando Histórico de Tamanho das Tabelas MySQL ---"
    begin
      @mysql_client.execute_query(%q{
        CREATE TABLE IF NOT EXISTS `size_table_history` (
          `data_coleta` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          `db_name` VARCHAR(255),
          `table_name` VARCHAR(255),
          `table_size_bytes` BIGINT
        );
      })

      @mysql_client.execute_query(%Q{
        INSERT INTO size_table_history (data_coleta, db_name, table_name, table_size_bytes)
        SELECT
          NOW(),
          table_schema,
          table_name,
          data_length + index_length
        FROM information_schema.tables
        WHERE table_schema = '#{@config.db_name}' AND table_type = 'BASE TABLE';
      })

      puts "Tamanhos das tabelas salvos em size_table_history com sucesso."
      @alert_messages << "INFO: Histórico de tamanho das tabelas MySQL atualizado com sucesso."
    rescue Mysql2::Error => e
      puts "Erro ao salvar tamanhos das tabelas MySQL: #{e.message}"
      @alert_messages << "ALERTA: Falha ao salvar o histórico de tamanho das tabelas MySQL: #{e.message}"
    end
  end
end

# --- 9. MySqlMonitor: Main orchestrator ---
class MySqlMonitor
  def initialize(frequency)
    @config = MySqlMonitorConfig.new
    @email_sender = EmailSender.new(@config)
    @alert_messages = []
    @frequency = frequency
  end

  def run
    alert_type_for_email = "GENERIC_MYSQL_MONITORING_ALERT_#{@config.db_name.upcase}"

    mysql_connection = nil
    if ['high', 'medium', 'low', 'health_check', 'table_size_history'].include?(@frequency)
      begin
        mysql_connection = MySqlConnection.new(@config)
        mysql_connection.connect
      rescue StandardError => e
        @email_sender.send_alert_email(
          "ALERTA CRÍTICO: Falha na Conexão MySQL - #{@config.db_name}",
          "Erro ao conectar ao banco de dados MySQL: #{e.message}",
          "MYSQL_DB_CONNECTION_ERROR_#{@config.db_name.upcase}"
        )
        puts "Erro crítico de conexão MySQL: #{e.message}"
        exit 1
      end
    end

    case @frequency
    when 'high'
      puts "Executando monitoramento de ALTA frequência para MySQL..."
      CriticalMetrics.new(mysql_connection, @config, @alert_messages).monitor
      alert_type_for_email = "MYSQL_HIGH_FREQ_ALERTS_#{@config.db_name.upcase}"
    when 'medium'
      puts "Executando monitoramento de MÉDIA frequência para MySQL..."
      PerformanceMetrics.new(mysql_connection, @config, @alert_messages).monitor
      alert_type_for_email = "MYSQL_MEDIUM_FREQ_ALERTS_#{@config.db_name.upcase}"
    when 'low'
      puts "Executando monitoramento de BAIXA frequência para MySQL..."
      OptimizationMetrics.new(mysql_connection, @config, @alert_messages).monitor
      alert_type_for_email = "MYSQL_LOW_FREQ_ALERTS_#{@config.db_name.upcase}"
    when 'health_check'
      puts "Executando TESTE DE SAÚDE E INTEGRIDADE DE DADOS DO MYSQL..."
      HealthChecker.new(mysql_connection, @config, @alert_messages).check
      alert_type_for_email = "MYSQL_HEALTH_CHECK_ALERTS_#{@config.db_name.upcase}"
    when 'table_size_history'
      puts "Executando Salvamento do Histórico de Tamanho das Tabelas MySQL..."
      TableSizeHistory.new(mysql_connection, @config, @alert_messages).save
      alert_type_for_email = "MYSQL_TABLE_SIZE_HISTORY_INFO_#{@config.db_name.upcase}"
    else
      puts "Nível de frequência desconhecido: #{@frequency}. Use 'high', 'medium', 'low', 'health_check' ou 'table_size_history'."
      exit 1
    end

    if @alert_messages.empty?
      puts "Status do MySQL para '#{@frequency}' frequência: OK. Nenhum alerta detectado."
    else
      subject = "ALERTA [#{@frequency.upcase}]: Problemas/Informações no MySQL - #{@config.db_name}"
      full_alert_body = "Monitoramento #{@frequency.upcase} em #{Time.now.strftime('%d/%m/%Y %H:%M:%S')} (Goiânia, GO, Brasil) detectou os seguintes problemas/informações:\n\n"
      @alert_messages.each do |msg|
        full_alert_body << "- #{msg}\n"
      end
      @email_sender.send_alert_email(subject, full_alert_body, alert_type_for_email)
    end

  rescue StandardError => e
    current_local_time = Time.now.strftime('%d/%m/%Y %H:%M:%S')
    error_message = "[#{current_local_time}] Ocorreu um erro inesperado durante o monitoramento #{@frequency}: #{e.message}\n#{e.backtrace.join("\n")}"
    @email_sender.send_alert_email(
      "ALERTA CRÍTICO: Erro no Script de Monitoramento MySQL",
      error_message,
      "MYSQL_SCRIPT_ERROR_#{@config.db_name.upcase}"
    )
    puts error_message
  ensure
    mysql_connection.close if mysql_connection
  end
end

# --- Script Entry Point ---
frequency_arg = ARGV[0] || 'high'
MySqlMonitor.new(frequency_arg).run