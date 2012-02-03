#!/usr/bin/env ruby
#
# StadiumVision Mobile:
# MongoDB Stats Import Script
#
# Mark Craig (mlcraig@cisco.com)
#

# import the required libraries
require 'rubygems' # not necessary for ruby 1.9+
require 'optparse'
require 'fileutils'
require 'zlib'
require 'json'
require 'mongo'

# define the default options
options = {
	:stats_import_dir => '.',
	:stats_file_extension => '.txt.gz',
	:mongo_host => '127.0.0.1',
	:mongo_port => 27017,
	:mongo_username => nil,
	:mongo_password => nil,
	:mongo_db_name => 'svm_stats',
	:mongo_collection_name => 'client_stats',
	:stats_archive_dir => './ARCHIVE',
	:stats_archive_file_prefix => 'ARCHIVE_',
	:log_file => nil,
	:verbose => false
}

# define the command-line options to parse
optparse = OptionParser.new do |opts|
	# get this script's name
	script_name = File.basename($0)
	
	# define the banner displayed as the help screen
	opts.banner = "\nusage: #{script_name} [options]\n\n"
	
	# define the command-line options
	opts.on( '--import-dir <DIRECTORY>', 'Stats import directory' ) do |dir|
		options[:stats_import_dir] = dir
	end
	
	opts.on( '--mongo-host <HOST>', 'MongoDB host (ip-address)' ) do |host|
		options[:mongo_host] = host
	end
	
	opts.on( '--mongo-port <PORT>', 'MongoDB tcp port number' ) do |port|
		options[:mongo_port] = port
	end
	
	opts.on( '--mongo-username <USERNAME>', 'MongoDB authentication username' ) do |username|
		options[:mongo_username] = username
	end
	
	opts.on( '--mongo-password <PASSWORD>', 'MongoDB authentication password' ) do |password|
		options[:mongo_password] = password
	end
	
	opts.on( '--mongo-db-name <NAME>', 'MongoDB database name' ) do |name|
		options[:mongo_db_name] = name
	end
	
	opts.on( '--mongo-collection <NAME>', 'MongoDB stats collection name' ) do |name|
		options[:mongo_collection_name] = name
	end
	
	opts.on( '--extension <FILE-EXTENSION>', 'Stats file extension' ) do |ext|
		options[:stats_file_extension] = ext
	end
	
	opts.on( '--archive-dir <DIRECTORY>', 'Stats archive file directory' ) do |dir|
		options[:stats_archive_dir] = dir
	end
	
	opts.on( '--archive-file-prefix <PREFIX>', 'Stats archive file prefix' ) do |prefix|
		options[:stats_archive_file_prefix] = prefix
	end
	
	opts.on( '--log-file <FILE>', 'Log output file' ) do |file|
		options[:log_file] = file
	end
	
	opts.on( '--verbose', 'Output more information' ) do
		options[:verbose] = true;
	end
	
	opts.on( '--help', 'Display this screen' ) do
		puts "#{opts}\n"
	end
end

# parse the command-line options
optparse.parse!

# display the options being used
puts
puts "Verbose output on" if options[:verbose] == true
puts "Stats import directory: #{options[:stats_import_dir]}"
puts "Stats file extension: #{options[:stats_file_extension]}"
puts "MongoDB host: #{options[:mongo_host]}"
puts "MongoDB port: #{options[:mongo_port]}"
puts "MongoDB database name: #{options[:mongo_db_name]}"
puts "MongoDB collection name: #{options[:mongo_collection_name]}"
puts "Stats archive directory: #{options[:stats_archive_dir]}"
puts "Stats archive file prefix: #{options[:stats_archive_file_prefix]}"
puts "Log file: #{options[:log_file]}" unless options[:log_file].nil?
puts

# get an array of all of the files in the import directory
stats_path = options[:stats_import_dir] + '/*' + options[:stats_file_extension]
stats_files = Dir.glob(stats_path);

# open a connection to mongodb
#db = Mongo::Connection.new("dbh75.mongolab.com", 27757).db("svm_stats")
conn = Mongo::Connection.new(options[:mongo_host], options[:mongo_port])
db = conn.db(options[:mongo_db_name])

# if a database username and password are given
if options[:mongo_username].nil? == false && options[:mongo_password].nil? == false
	# authenticate with the database
	auth = db.authenticate(options[:mongo_username], options[:mongo_password])
	
	# if the authentication was successful
	if auth == true
		puts "Authentication successful"
	else
		puts "Authentication failed"
		exit 1
	end
end

# get the collection used for import stats
stats_collection = db[options[:mongo_collection_name]]

# create the stats archive directory if it does not exist
if options[:stats_archive_dir].nil? == true
	FileUtils.mkdir_p options[:stats_archive_dir]
end

# process each stats file
puts "Processing stats files: #{stats_files.length}\n\n"
stats_files.each do |file|
	# display the file being processed
	#puts "Processing stats file: #{file}"
	
	# get the server import time for this stats file
	import_time = Time.now
	
	# copy the stats file to the stats archive directory
	if options[:stats_archive_dir].nil? == false
		# get the base stats filename
		stats_file_basename = File.basename(file)
		
		#gzip_data = File.open(file, 'rb') { |io| io.read }
		
		json_string = ""
		File.open(file) do |f|
			gz = Zlib::GzipReader.new(f)
			json_string = gz.read
			gz.close
		end
		
		# parse the json string into an array of hash structures
		stats_array = JSON.parse(json_string)
		
		# display the stats array info
		#puts "Num stats entries = #{stats_array.length}"
		#puts "\nJSON STATS:\n\n#{stats_array.to_s}"
		
		# spin through each entry in the stats array
		stats_array.each do |stats|
			# if the stats entry does not have a valid timestamp
			if stats['timestamp'].nil? || stats['timestamp']['date'].nil?
				# create a timestamp entry
				stats['timestamp'] = {}
				stats['timestamp']['date'] = 0
			end

			# convert the client's timestamp into a Time object
			stats['timestamp']['date'] = Time.at(stats['timestamp']['date'].to_f / 1000).to_s
			
			# set the import timestamp (normalized server time)
			stats['timestamp']['importDate'] = import_time
			
			# display the stats hash
			#puts "STATS = #{stats}"
		end
		
		# display the json stats
		#if json[0]['wifi'] && json[0]['wifi']['rssi']
		#	puts "First entry data = #{json[0]['wifi']['rssi']}"
		#end
		
		# insert the array of stats into the database
		puts "IMPORTING STATS: #{stats_array.length}"
		stats_collection.insert(stats_array)
		
		# move the stats file to the archive directory with archive filename prefix
		FileUtils.mv file, "#{options[:stats_archive_dir]}/#{options[:stats_archive_file_prefix]}#{stats_file_basename}"
	end
		
end

# close the connection to mongodb
conn.close


