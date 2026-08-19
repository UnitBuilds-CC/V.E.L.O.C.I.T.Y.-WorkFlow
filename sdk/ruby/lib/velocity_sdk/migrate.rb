# frozen_string_literal: true

# Velocity Ruby Migration Tool
#
# Scans a Ruby codebase for Temporal, Restate, or DBOS workflow patterns
# and converts them to Velocity Ruby SDK workflows.
#
# Usage:
#   ruby sdk/ruby/lib/velocity_sdk/migrate.rb --src ./my_project --from temporal
#   ruby sdk/ruby/lib/velocity_sdk/migrate.rb --src workflow.rb --from auto
#   ruby sdk/ruby/lib/velocity_sdk/migrate.rb --detect ./my_project

module VelocitySDK
  module Migrate
    # ─── Pattern Definition ────────────────────────────────────────────────

    MigrationPattern = Struct.new(:name, :source_pattern, :target_template, :source_framework)

    # ─── Temporal → Velocity Patterns ──────────────────────────────────────

    TEMPORAL_PATTERNS = [
      MigrationPattern.new('temporal-require-workflow',
        /require\s+['"]temporal\/workflow['"]/,
        "require 'velocity_sdk'"),
      MigrationPattern.new('temporal-require-activity',
        /require\s+['"]temporal\/activity['"]/,
        "require 'velocity_sdk'"),
      MigrationPattern.new('temporal-require-client',
        /require\s+['"]temporal\/client['"]/,
        "require 'velocity_sdk/client'"),
      MigrationPattern.new('temporal-require-worker',
        /require\s+['"]temporal\/worker['"]/,
        "require 'velocity_sdk/worker'"),

      # Module/class replacements
      MigrationPattern.new('temporal-workflow-module',
        /include\s+Temporal::Workflow/,
        'include VelocitySDK::Workflow'),
      MigrationPattern.new('temporal-activity-module',
        /include\s+Temporal::Activity/,
        'include VelocitySDK::Activity'),

      # Method call replacements
      MigrationPattern.new('temporal-execute-activity',
        /execute_activity\s*\(\s*['"]?(\w+)['"]?\s*/,
        'ctx.execute_activity(\1, '),
      MigrationPattern.new('temporal-sleep',
        /Temporal::Workflow\.sleep\s*\(/,
        'ctx.sleep('),
      MigrationPattern.new('temporal-side-effect',
        /Temporal::Workflow\.side_effect\s*\(/,
        'ctx.side_effect('),
      MigrationPattern.new('temporal-signal',
        /Temporal::Workflow::Signal/,
        'VelocitySDK::Signal'),
      MigrationPattern.new('temporal-query',
        /Temporal::Workflow::Query/,
        'VelocitySDK::Query'),

      # Client/Worker
      MigrationPattern.new('temporal-client-new',
        /Temporal::Client\.new\s*\(/,
        'VelocitySDK::VelocityClient.new('),
      MigrationPattern.new('temporal-worker-new',
        /Temporal::Worker\.new\s*\(/,
        'VelocitySDK::Worker.new('),
      MigrationPattern.new('temporal-register-workflow',
        /register_workflow\s*\(/,
        'register_workflow('),
      MigrationPattern.new('temporal-register-activity',
        /register_activity\s*\(/,
        'register_activity('),
      MigrationPattern.new('temporal-start-workflow',
        /client\.execute_workflow\s*\(/,
        'client.execute_workflow('),
    ].freeze

    # ─── Restate → Velocity Patterns ───────────────────────────────────────

    RESTATE_PATTERNS = [
      MigrationPattern.new('restate-require',
        /require\s+['"]restate['"]/,
        "require 'velocity_sdk'"),
      MigrationPattern.new('restate-module',
        /include\s+Restate::/,
        'include VelocitySDK::'),
      MigrationPattern.new('restate-ctx-run',
        /context\.run\s*\(/,
        'ctx.execute_activity('),
      MigrationPattern.new('restate-ctx-call',
        /context\.call\s*\(/,
        'ctx.execute_activity('),
      MigrationPattern.new('restate-ctx-sleep',
        /context\.sleep\s*\(/,
        'ctx.sleep('),
      MigrationPattern.new('restate-ctx-get',
        /context\.get\s*\(\s*['"](\w+)['"]/,
        "ctx.get_state('\\1')"),
      MigrationPattern.new('restate-ctx-set',
        /context\.set\s*\(\s*['"](\w+)['"]/,
        "ctx.set_state('\\1'"),
    ].freeze

    # ─── DBOS → Velocity Patterns ──────────────────────────────────────────

    DBOS_PATTERNS = [
      MigrationPattern.new('dbos-require',
        /require\s+['"]dbos['"]/,
        "require 'velocity_sdk'"),
      MigrationPattern.new('dbos-workflow-decorator',
        /extend\s+DBOS::Workflow/,
        'include VelocitySDK::Workflow'),
      MigrationPattern.new('dbos-transaction-decorator',
        /extend\s+DBOS::Transaction/,
        'include VelocitySDK::Activity'),
      MigrationPattern.new('dbos-sleep',
        /DBOS\.sleep\s*\(/,
        'ctx.sleep('),
      MigrationPattern.new('dbos-recv',
        /DBOS\.recv\s*\(/,
        'ctx.recv('),
      MigrationPattern.new('dbos-set-event',
        /DBOS\.set_event\s*\(/,
        'ctx.set_event('),
      MigrationPattern.new('dbos-get-event',
        /DBOS\.get_event\s*\(/,
        'ctx.get_event('),
    ].freeze

    ALL_PATTERNS = {
      'temporal' => TEMPORAL_PATTERNS,
      'restate'  => RESTATE_PATTERNS,
      'dbos'     => DBOS_PATTERNS,
    }.freeze

    # ─── Framework Detection ───────────────────────────────────────────────

    def self.detect_framework(content)
      scores = { 'temporal' => 0, 'restate' => 0, 'dbos' => 0 }
      evidence = { 'temporal' => [], 'restate' => [], 'dbos' => [] }

      # Temporal
      if content.match?(/temporal\/workflow/) || content.match?(/Temporal::Workflow/)
        scores['temporal'] += 3
        evidence['temporal'] << 'Temporal workflow import'
      end
      if content.match?(/Temporal::Activity/)
        scores['temporal'] += 2
        evidence['temporal'] << 'Temporal activity module'
      end
      if content.match?(/execute_activity/)
        scores['temporal'] += 1
        evidence['temporal'] << 'execute_activity call'
      end

      # Restate
      if content.match?(/require\s+['"]restate['"]/) || content.match?(/Restate::/)
        scores['restate'] += 3
        evidence['restate'] << 'Restate import'
      end
      if content.match?(/context\.run\s*\(/)
        scores['restate'] += 1
        evidence['restate'] << 'context.run()'
      end

      # DBOS
      if content.match?(/require\s+['"]dbos['"]/) || content.match?(/DBOS::/)
        scores['dbos'] += 3
        evidence['dbos'] << 'DBOS import'
      end
      if content.match?(/DBOS\.sleep/)
        scores['dbos'] += 1
        evidence['dbos'] << 'DBOS.sleep'
      end

      best = scores.max_by { |_, v| v }
      total = scores.values.sum
      confidence = total > 0 ? best[1].to_f / total : 0.0

      { framework: best[0], confidence: confidence, evidence: evidence[best[0]] }
    end

    # ─── File Migration ────────────────────────────────────────────────────

    def self.migrate_file(content, source_framework)
      result = { success: true, detected: '', transformations: 0, error: nil }

      if source_framework == 'auto'
        detection = detect_framework(content)
        result[:detected] = detection[:framework]
        if detection[:confidence] < 0.3
          return [content, { success: false, error: "Low confidence: #{detection[:framework]}" }]
        end
        source_framework = detection[:framework]
      else
        result[:detected] = source_framework
      end

      patterns = ALL_PATTERNS[source_framework]
      unless patterns
        return [content, { success: false, error: "Unknown framework: #{source_framework}" }]
      end

      migrated = content.dup
      count = 0
      patterns.each do |p|
        new_text = migrated.gsub(p.source_pattern, p.target_template)
        count += 1 if new_text != migrated
        migrated = new_text
      end
      result[:transformations] = count

      [migrated, result]
    end

    # ─── Project Scanner ───────────────────────────────────────────────────

    SKIP_DIRS = %w[vendor .git node_modules tmp log].freeze

    def self.scan_ruby_files(root_dir)
      files = []
      Dir.glob(File.join(root_dir, '**', '*.rb')).each do |f|
        next if SKIP_DIRS.any? { |d| f.include?("/#{d}/") }
        files << f
      end
      files
    end

    def self.has_workflow_content?(content)
      indicators = ['Temporal::', 'Restate::', 'DBOS::', 'temporal/', 'execute_activity', 'context.run']
      indicators.any? { |i| content.include?(i) }
    end

    # ─── Bulk Migration ────────────────────────────────────────────────────

    def self.bulk_migrate(source_dir, output_dir, from, dry_run: false)
      files = scan_ruby_files(source_dir)
      results = { total: files.size, migrated: 0, failed: 0, skipped: 0, details: [] }

      files.each do |file_path|
        content = File.read(file_path)
        unless has_workflow_content?(content)
          results[:skipped] += 1
          next
        end

        migrated, result = migrate_file(content, from)

        if result[:success] && !dry_run
          rel_path = file_path.sub("#{source_dir}/", '')
          out_path = File.join(output_dir, rel_path)
          FileUtils.mkdir_p(File.dirname(out_path))
          File.write(out_path, migrated)
          results[:migrated] += 1
        elsif result[:success]
          results[:migrated] += 1
        else
          results[:failed] += 1
        end

        results[:details] << result.merge(path: file_path)
      end

      results
    end
  end
end

# ─── CLI Entry Point ─────────────────────────────────────────────────────────

if __FILE__ == $PROGRAM_NAME
  require 'optparse'
  require 'fileutils'

  options = { from: 'auto' }
  OptionParser.new do |opts|
    opts.banner = "Velocity Ruby Migration Tool\n\nUsage: ruby migrate.rb [options]"
    opts.on('--src PATH', 'Source file or directory') { |v| options[:src] = v }
    opts.on('--from FRAMEWORK', 'Source: temporal, restate, dbos, auto') { |v| options[:from] = v }
    opts.on('--output PATH', '-o PATH', 'Output file or directory') { |v| options[:output] = v }
    opts.on('--dry-run', 'Detect without writing') { options[:dry_run] = true }
    opts.on('--detect', 'Detect framework in directory') { options[:detect] = true }
  end.parse!

  unless options[:src]
    $stderr.puts 'Error: --src is required'
    exit 1
  end

  if options[:detect]
    files = VelocitySDK::Migrate.scan_ruby_files(options[:src])
    puts "Scanning #{files.size} Ruby files in #{options[:src]}..."
    files.each do |f|
      content = File.read(f)
      d = VelocitySDK::Migrate.detect_framework(content)
      if d[:confidence] > 0.3
        rel = f.sub("#{options[:src]}/", '')
        puts "  #{rel}: #{d[:framework]} (#{(d[:confidence] * 100).round}%)"
      end
    end
    exit 0
  end

  if File.file?(options[:src])
    content = File.read(options[:src])
    migrated, result = VelocitySDK::Migrate.migrate_file(content, options[:from])
    unless result[:success]
      $stderr.puts "Failed: #{result[:error]}"
      exit 1
    end
    if options[:output]
      File.write(options[:output], migrated)
      puts "Written to: #{options[:output]}"
    else
      puts migrated
    end
    puts "\nDetected: #{result[:detected]}"
    puts "Transformations: #{result[:transformations]}"
    exit 0
  end

  if File.directory?(options[:src])
    output_dir = options[:output] || File.join(File.dirname(options[:src]), 'velocity-migrated')
    puts "Scanning: #{options[:src]}"
    puts "Output: #{options[:dry_run] ? '(dry run)' : output_dir}"
    puts "Source framework: #{options[:from]}\n\n"

    results = VelocitySDK::Migrate.bulk_migrate(
      options[:src], output_dir, options[:from], dry_run: options[:dry_run]
    )
    puts "Results:"
    puts "  Total: #{results[:total]}"
    puts "  Migrated: #{results[:migrated]}"
    puts "  Failed: #{results[:failed]}"
    puts "  Skipped: #{results[:skipped]}"
    results[:details].each do |r|
      status = r[:success] ? 'OK' : 'FAIL'
      puts "  [#{status}] #{r[:path]} (#{r[:detected]}, #{r[:transformations]} changes)"
    end
  end
end
