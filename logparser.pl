#!/usr/local/bin/perl5 -w


####################
#  Perl modules
####################
use strict;
use Getopt::Long qw(:config no_ignore_case);
use Time::HiRes qw(gettimeofday);
use Data::Dumper;
use HTTP::Date;

####################
#  Global variables
####################
( my $pgm = $0 ) =~ s!.*/(.*)!$1!;    # Extract my name
my %gOptions;                         # For command line arguments
my %job_data;									# Fore recording job execution data
my %lb_data;									# Fore recording load balance data
my @job_lines;   								# single job execution bcp lines
my @lb_lines;   								# load balance bcp lines
my $date;
my $need_unzip = -1;
my $split = "|";
my %ck_method = ();
my %max_line_count = ("single_job" => 1,	"load_balance" => 1);
my %data_sequence = ();
my %cur_count = ("single_job" => 0,	"load_balance" => 0);
my %cols_count = ("single_job" => 7,	"load_balance" => 5);  # column count that we want to get
my $tmp_key = "";
my $err_log = "tem_err.bcp";
exit Main();

######################################################################
#                Functions
######################################################################

####################
# Function:	usage
####################
sub Usage {
    my ($pgm) = @_;
    print <<"END_USAGE";
Usage: $pgm [options] -d directory -p pattern
Options:
	-directory		file location
	-pattern		file name pattern

END_USAGE
    exit(1);
}

####################
# Function:	Setup
#
# Configure the various resources based on:
# 1. command line option
# 2. environment variable
# 3. hard-coded default
####################
sub Setup {
    my ($pgm) = @_; 

    # parse command line which overrides environment
    GetOptions(
        \%gOptions, 'directory=s', 'pattern=s',
    ) or Usage($pgm);  

	# set default values
	$gOptions{'pattern'}
        = defined( $gOptions{'pattern'}  )
        ? $gOptions{'pattern'} : "event_demon.*";
	
    # need the file directory to continue
    Usage($pgm) if ( ! defined( $gOptions{'directory'} ) );
}

####################
# Function:	GetUserInfo
####################
sub GetUserInfo{
	my $user = `/usr/ucb/whoami`;
	chomp($user);
	print "Current user:\t$user\n";
	$user;
}

####################
# Function:	GetLogList
# 
# #According to the given pattern to find matched files list
####################
sub GetLogList{
	my($dir,$pattern) = @_;
	print "$dir\n";
	print "$pattern\n";
	opendir(DH, "$dir") or die "$! :$dir\n" ;
	my @list =  sort grep {/^$pattern$/ && -f "$dir/$_" } readdir(DH);
	closedir(DH);
	print Dumper(\@list);
	\@list;
}

sub GetTempLogData{
	
}

####################
# Function:	ParseLogs
# 
# #Parse log collection
####################
sub ParseLogCollection{
	my($dir,$logList) = @_;
	@job_lines = ();
	my $total_logs_count = @$logList;
	my $cur = 1;
	foreach my $file(@$logList){
		print "$cur\/$total_logs_count: $file => ";
		$file = "$dir/$file";
		$need_unzip = -1;
		ParseLog($file);
		$cur++;
	}
}

####################
# Function:	ParseLog
# 
# #Parse ufda log
####################
sub ParseLog{
	my($log) = @_;
	print "$log\n";
	$log = &Unzip($log);
	FetchLogInfo($log);
	RemoveFile($log) if($need_unzip >= 0);
}

####################
# Function:	UnzipLog
####################
sub Unzip{
	my ($filename) = @_;
	my $is_gzip_suceess = 1;
	$need_unzip = index($filename, ".gz" );    # is zip file?
	if( $need_unzip >= 0){
		print "###############\nUnzipping...\n";
		my @filename = split(".gz",$filename);
		system("gunzip -c $filename > $filename[0]");
		$filename = $filename[0];
		$is_gzip_suceess = $? >> 8;
		if($is_gzip_suceess != 0){
			print "Gunzip $filename.gz failed!\n";
			last;		
		}
		print "Unziped.\n";
	}
	$filename;
}

#####################################
# Function:	PrepareUnfinishedJobsData
#####################################
sub PrepareUnfinishedJobsData{
	if(-e $err_log){
		open ELOG, $err_log or die ("Could not open file $err_log\n");
		while(<ELOG>){
			chomp;
			#log items 
			my @err_log_vars = split(/\|/,$_);
			
			my $job_name = $err_log_vars[0];
			my $machine = $err_log_vars[1];
			my $machine_time = $err_log_vars[2];
			my $start_time = $err_log_vars[3];
			my $running_time = $err_log_vars[4];
			my $finish_time = $err_log_vars[5];
			my $status = $err_log_vars[6];
			my $key = $job_name;
			
			my %block = ();
			my @data = ("","","","","","","");
			$block{"cnt"} = 0;
			$block{"data"} = \@data;
			$job_data{$key} = \%block;
			
			AddData($job_data{$key}, "job_name", $job_name, "single_job") if($job_name);
			AddData($job_data{$key}, "machine", $machine, "single_job") if($machine);
			AddData($job_data{$key}, "machine_time", $machine_time, "single_job") if($machine_time);
			AddData($job_data{$key}, "start_time", $start_time, "single_job") if($start_time);
			AddData($job_data{$key}, "running_time", $running_time, "single_job") if($running_time);
			AddData($job_data{$key}, "finish_time", $finish_time, "single_job") if($finish_time);
			AddData($job_data{$key}, "status", $status, "single_job") if($status);
		}
		close ELOG or die "Failure closing log file: $!\n";
		RemoveFile("$err_log");
	}
}

####################
# Function:	FetchLogInfo
####################
sub FetchLogInfo{
	my($log) = @_;
	%job_data = ();
	%lb_data = ();
	
	# add unfinished jobs data
	PrepareUnfinishedJobsData();
	
	$log =~	/(\d+)$/;
	$date = $1;
	my $job_outputfile = $log.".sj_bcp";				#output file for single job execution
	my $se_error_bcp = $log.".sj_error";
	my $lb_error_bcp = $log.".lb_error";
	my $lb_outputfile = $log.".lb_bcp";						#output file for load balance
	my $needToWrite = 0;
	my $line_count = 0;
	open LOG, $log or die ("Could not open file $log\n");	
	while(<LOG>){
		chomp;
		print "$line_count rows\n" if ( ( ++$line_count % 2000 ) == 0 );
		FetchStartInfo($_);
		if($cur_count{"single_job"} >= $max_line_count{"single_job"}){
			PrepareBCPForSingleJob();
			WriteBcpForSingleJob($job_outputfile);
		}
		if($cur_count{"load_balance"} >= $max_line_count{"load_balance"}){
			PrepareBCPForLoadBalance();
			WriteBcpForLoadBalance($lb_outputfile);
		}
	}
	PrepareBCPForSingleJob();
	WriteBcpForSingleJob($job_outputfile);
	PrepareBCPForLoadBalance();
	WriteBcpForLoadBalance($lb_outputfile);
	print "\n$job_outputfile\n";
	print "$lb_outputfile\n";
	close LOG or die "Failure closing log file: $!\n";
	WriteErrorBcpForSingleJob($err_log);
	WriteErrorBcpForLoadBalance($lb_error_bcp);
	%job_data = ();
	%lb_data = ();
}

####################
# Function:	RemoveFile
####################
sub RemoveFile{
	my ($file) = @_;
	system("rm $file");
	my $dfiles = $? >> 8;
	if($dfiles !=0){
		print "Failed to delete $file!\n";
	}
}

####################
# Function:	FetchStartInfo
####################
sub FetchStartInfo{
	my ($log_line) = @_;
	if( (!$ck_method{"switch"}) && $log_line =~ /\[(.*)\]\s+\[\d+\]\s+\[(\w+)\s+connected\s+for\s+(\w+)\]/){
		my $machine_time = $1;
		my $machine = $2;
		my $job_name = $3;
		$machine_time = ConvertLogTime($machine_time);
		my $key = $job_name;
		my %block = ();
		my @data = ("","","","","","","");
		$block{"cnt"} = 0;
		$block{"data"} = \@data;
		$job_data{$key} = \%block;
		AddData($job_data{$key}, "job_name", $job_name, "single_job") if($job_name);
		AddData($job_data{$key}, "machine", $machine, "single_job") if($machine);
		AddData($job_data{$key}, "machine_time", $machine_time, "single_job") if($machine_time);
		return;
	}
	
	if($log_line =~ /\[(.*)\]\s+\[\d+\]\s+EVENT:\s+STARTJOB\s+JOB:\s+(\w+)/){
		my $start_time = ConvertLogTime($1);
		my $job_name = $2;
		my $machine = "";
		my $machine_time = "";
		my $key = $job_name;
		my %block = ();
		my @data = ("","","","","","","");
		$block{"cnt"} = 0;
		$block{"data"} = \@data;
		$job_data{$key} = \%block;
		AddData($job_data{$key}, "job_name", $job_name, "single_job")  if($job_name);
		AddData($job_data{$key}, "machine", $machine, "single_job");
		AddData($job_data{$key}, "machine_time", $machine_time, "single_job");
		AddData($job_data{$key}, "start_time", $start_time, "single_job") if($start_time);
		return;
	}
	
	if( $log_line =~ /\[(.*)\]\s+\[\d+\]\s+EVENT:\s+CHANGE_STATUS\s+STATUS:\s+STARTING\s+JOB:\s+(\w+)/ ){
		my $start_time = ConvertLogTime($1);
		my $key = $2;
		if($job_data{$key} && $job_data{$key}{"data"}->[$data_sequence{"single_job"}{"job_name"}] ){
			if(!$job_data{$key}{"data"}->[$data_sequence{"single_job"}{"start_time"}]){
				AddData($job_data{$key}, "start_time", $start_time, "single_job") if($start_time);
			}
		}
		return;
	}
	
	if( $log_line =~ /\[(.*)\]\s+\[\d+\]\s+EVENT:\s+CHANGE_STATUS\s+STATUS:\s+RUNNING\s+JOB:\s+(\w+)/ ){
		my $running_time = ConvertLogTime($1);
		my $key = $2;
		if($job_data{$key} && $job_data{$key}{"data"}->[$data_sequence{"single_job"}{"job_name"}] ){
			if(!$job_data{$key}{"data"}->[$data_sequence{"single_job"}{"running_time"}]){
				# get the first running time
				AddData($job_data{$key}, "running_time", $running_time, "single_job") if($running_time);
			}
		}
		return;
	}
	
	if( $log_line =~ /\[(.*)\]\s+\[\d+\]\s+EVENT:\s+CHANGE_STATUS\s+STATUS:\s+(\w+)\s+JOB:\s+(\w+)/ ){
		my $finish_time = ConvertLogTime($1);
		my $status = $2;
		my $key = $3;
		return if($status ne "SUCCESS" && $status ne "FAILURE" && $status ne "FAILED");
		if($job_data{$key} && $job_data{$key}{"data"}->[$data_sequence{"single_job"}{"job_name"}] ){			
			# get the first finish time
			AddData($job_data{$key}, "finish_time", $finish_time, "single_job") if($finish_time);				
			AddData($job_data{$key}, "status", $status, "single_job") if($status);
		}
		return;
	}
	
	if( $log_line =~ /\[(\d+:\d+:\d+.\d+)\]\s+\[\d+\]\s+.*AUTO_JOB_NAME=(\w+)/ ){
		my $start_time = ConvertLogTime($1);
		my $job_name = $2;
		my $key = $job_name;
		my $method = "user defined";
		$tmp_key = $key;
		my %block = ();
		my @data = ("","","","","");
		$block{"cnt"} = 0;
		$block{"data"} = \@data;
		$lb_data{$key} = \%block;
		AddData($lb_data{$key}, "job_name", $job_name, "load_balance") if($job_name);
		AddData($lb_data{$key}, "start_time", $start_time, "load_balance") if($start_time);
		AddData($lb_data{$key}, "method", $method, "load_balance") if($method);
		return;
	}
	
	if( $log_line =~ /\[(\d+:\d+:\d+.\d+)\]\s+\[\d+\]\s+Checking Machine usages using (\w+) Method/ ){
		my $time = ConvertLogTime($1);
		my $method = $2;
		$ck_method{"start_time"} = $time;
		$ck_method{"method"} = $method;
		$ck_method{"switch"} = 1;
		return;
	}
	
	if( $log_line =~ /\[(\d+:\d+:\d+.\d+)\]\s+\[\d+\]\s+\[(\w+) connected for (.*)\]/ ){
		my $end_time = ConvertLogTime($1);
		my $machine = $2;
		my $job_name = $3;
		my $key = $job_name;
		my %block = ();
		my @data = ("","","","","");
		$block{"cnt"} = 0;
		$block{"data"} = \@data;
		$lb_data{$key} = \%block;
		AddData($lb_data{$key}, "job_name", $job_name, "load_balance") if($job_name);
		AddData($lb_data{$key}, "start_time", $ck_method{"start_time"}, "load_balance") if($ck_method{"start_time"});
		AddData($lb_data{$key}, "end_time", $end_time, "load_balance") if($end_time);
		AddData($lb_data{$key}, "machine", $machine, "load_balance") if($machine);
		AddData($lb_data{$key}, "method", $ck_method{"method"}, "load_balance") if($ck_method{"method"});
		$ck_method{"switch"} = 0;
		%ck_method = ();
		return;
	}
	
	if( $log_line =~ /\[(\d+:\d+:\d+.\d+)\]\s+\[\d+\]\s+Returned Machine:\s+(\w+)/ ){
		my $end_time = ConvertLogTime($1);
		my $machine = $2;
		my $key = $tmp_key;
		if($key ne "" && $lb_data{$key} && $lb_data{$key}{"data"}->[$data_sequence{"load_balance"}{"job_name"}] ){
			AddData($lb_data{$key}, "end_time", $end_time, "load_balance") if($end_time);
			AddData($lb_data{$key}, "machine", $machine, "load_balance");
		}
		$tmp_key = "";
		return;
	}
}


####################
# Function:	AddData
####################
sub AddData{
	my ($block, $key, $value, $type) = @_;
	if(!$block->{$key}){
		if($block->{"cnt"} < $cols_count{$type} ){
			# only the first time update the value will update the count
			$block->{"cnt"}++ if(!$block->{"data"}->[$data_sequence{$type}{$key}]);			
		}
		if($block->{"cnt"} == $cols_count{$type} ){
			$cur_count{$type}++;
		}
	}
	
	$block->{"data"}->[$data_sequence{$type}{$key}] = $value;
}


####################
# Function:	PrepareBCPForSingleJob
####################
sub PrepareBCPForSingleJob{
	my $count = 0;
	my @delete_list = ();
	my $cache_count = keys %job_data;
	foreach my $key(keys %job_data){
		if( $job_data{$key}->{"cnt"} >= $cols_count{"single_job"} ){
			$count = @job_lines;
			if( $count < $max_line_count{"single_job"} ){	
				$job_lines[$count] = $job_data{$key}->{"data"}[0].$split
											.$job_data{$key}->{"data"}[1].$split
											.$job_data{$key}->{"data"}[2].$split
											.$job_data{$key}->{"data"}[3].$split
											.$job_data{$key}->{"data"}[4].$split
											.$job_data{$key}->{"data"}[5].$split
											.$job_data{$key}->{"data"}[6]."\n";
											#.$split;
				#if( $job_data{$key}->{"machine_time"} ){
				#		$job_lines[$count] = $job_lines[$count].CalcTimeSpan($job_data{$key}->{"machine_time"}, $job_data{$key}->{"finish_time"})."\n";
				#}
				#else{
				#		$job_lines[$count] = $job_lines[$count].CalcTimeSpan($job_data{$key}->{"start_time"}, $job_data{$key}->{"finish_time"})."\n";
				#}
				push @delete_list, $key;
			}
		}
	}
	foreach my $key (@delete_list){
		delete $job_data{$key};
	}
	$cur_count{"single_job"} = 0;
}

####################
# Function:	PrepareBCPForLoadBalance
####################
sub CalcTimeSpan{
	my ($start, $end) = @_;
	return 0 if(!$start || !$end);
	$start =~ s/:(\d\d\d)$/.$1/;
	$end =~ s/:(\d\d\d)$/.$1/;
	my $stime = HTTP::Date::str2time($start, '+0000');
	my $etime = HTTP::Date::str2time($end, '+0000');
	my $timeDelta = sprintf("%.4f", $etime - $stime);
	my $span = "$timeDelta";
}

####################
# Function:	PrepareBCPForLoadBalance
####################
sub PrepareBCPForLoadBalance{
	my $count = 0;
	my @delete_list = ();
	my $cache_count = keys %lb_data;
	foreach my $key(keys %lb_data){
		if( $lb_data{$key}->{"cnt"} >= $cols_count{"load_balance"} ){
			$count = @lb_lines;
			if( $count < $max_line_count{"load_balance"} ){
				$lb_lines[$count] = $lb_data{$key}->{"data"}[0].$split
											.$lb_data{$key}->{"data"}[1].$split
											.$lb_data{$key}->{"data"}[2].$split
											.$lb_data{$key}->{"data"}[3].$split
											.$lb_data{$key}->{"data"}[4]."\n";
										#	.$split
										#	.CalcTimeSpan($lb_data{$key}->{"start_time"}, $lb_data{$key}->{"end_time"})."\n";
				push @delete_list, $key;
			}
		}
	}
	foreach my $key (@delete_list){
		delete $lb_data{$key};
	}
	$cur_count{"load_balance"} = 0;
}

#####################
# Function:	WriteErrorBcpForSingleJob
#####################
sub WriteErrorBcpForSingleJob{
	my ($se_error_bcp) = @_;
	if(0 != (scalar keys %job_data)){
		open ERRORBCP, ">>$se_error_bcp" or die "Cannot open $se_error_bcp for write :$!";
		foreach my $key(keys %job_data){
			#print ERRORBCP $job_data{$key}."\n";
			#my $block = $job_data{$key};
			#print Dumper($block);
			print ERRORBCP $job_data{$key}->{"data"}[0].$split
						  .$job_data{$key}->{"data"}[1].$split
						  .$job_data{$key}->{"data"}[2].$split
						  .$job_data{$key}->{"data"}[3].$split
						  .$job_data{$key}->{"data"}[4].$split
						  .$job_data{$key}->{"data"}[5].$split
						  .$job_data{$key}->{"data"}[6];
			#foreach my $subKey (keys %$block){
			#	print ERRORBCP $block->{$subKey}.$split if($subKey ne "cnt")
			#}
			print ERRORBCP "\n";
		}
		close ERRORBCP or die "Failure closing log file: $!\n";
	}
}

#####################
# Function:	WriteErrorBcpForLoadBalance
#####################
sub WriteErrorBcpForLoadBalance{
	my ($lb_error_bcp) = @_;
	if(0 != (scalar keys %lb_data)){
		open ERRORBCP, ">>$lb_error_bcp" or die "Cannot open $lb_error_bcp for write :$!";
		foreach my $key(keys %lb_data){
			#print ERRORBCP $lb_data{$key}."\n";
			#my $block = $lb_data{$key};
			#print Dumper($block);
			#foreach my $subKey (keys %$block){
			#	print ERRORBCP $block->{$subKey}.$split if($subKey ne "cnt")
			#}
			print ERRORBCP $lb_data{$key}->{"data"}[0].$split
						  .$lb_data{$key}->{"data"}[1].$split
						  .$lb_data{$key}->{"data"}[2].$split
						  .$lb_data{$key}->{"data"}[3].$split
						  .$lb_data{$key}->{"data"}[4];
						  
			print ERRORBCP "\n";
		}
		close ERRORBCP or die "Failure closing log file: $!\n";
	}
}

####################
# Function:	WriteBcpForSingleJob
####################
sub WriteBcpForSingleJob{
	my ($file) = @_;		
	open OUT, ">>$file" or die "Cannot open $file for write :$!";
	foreach my $line(@job_lines){
		print OUT $line;
	}
	close OUT or die "Failure closing bcp file: $!\n";
	@job_lines = ();
}

####################
# Function:	WriteBcpForLoadBalance
####################
sub WriteBcpForLoadBalance{
	my ($file) = @_;
	open OUT, ">>$file" or die "Cannot open $file for write :$!";
	foreach my $line(@lb_lines){
		print OUT $line;
	}
	close OUT or die "Failure closing bcp file: $!\n";
	@lb_lines = ();
}

####################
# Function:	ConvertLogTime
####################
sub ConvertLogTime{
	my ($log) = @_;
	$date =~ /(\d\d)(\d\d)(\d\d\d\d)/;
	my $year = $3;
	my $month = $1;
	my $day = $2;
	$log =~ /(\d+):(\d+):(\d+).(\d\d\d)/;
	my $hour = $1;
	my $min = $2;
	my $sec = $3;
	my $millisec = $4;
	
	my $strtime = $year."/".$month."/".$day." ".$hour.":".$min.":".$sec;
	if($hour eq "00" && $min eq "00" && $sec eq "00"){
		my $time = HTTP::Date::str2time($strtime, '+0000');
		$time = $time + 24 * 60 * 60;
		$strtime = HTTP::Date::time2str($time);
		($year, $month, $day, $hour, $min, $sec) = HTTP::Date::parse_date($strtime);
		$strtime = $year."/".$month."/".$day." ".$hour.":".$min.":".$sec;
	}
	$strtime = $strtime.":".$millisec;
}

####################
# Function:	CalcRunTime
####################
sub CalcRunTime{
	my ($start_sec,$start_usec) = @_;
	my ($end_sec, $end_usec) = gettimeofday();
	my $timeDelta = (($end_usec - $start_usec) / 1000 + ($end_sec - $start_sec) * 1000) / 1000;
	my $time_min = int($timeDelta/60);
	my $time_sec = $timeDelta - $time_min * 60;
	print "* Dealing Time:\t".$time_min."m".$time_sec."s\n";
}

######################################################################
#                    Main
######################################################################
sub Main {
	# validate command line options and set %gOptions
    Setup($pgm);
	$data_sequence{"single_job"}{"job_name"} = 0;
	$data_sequence{"single_job"}{"machine"} = 1;
	$data_sequence{"single_job"}{"machine_time"} = 2;
	$data_sequence{"single_job"}{"start_time"} = 3;
	$data_sequence{"single_job"}{"running_time"} = 4;
	$data_sequence{"single_job"}{"finish_time"} = 5;
	$data_sequence{"single_job"}{"status"} = 6;
	
	$data_sequence{"load_balance"}{"method"} = 0;
	$data_sequence{"load_balance"}{"job_name"} = 1;
	$data_sequence{"load_balance"}{"start_time"} = 2;
	$data_sequence{"load_balance"}{"end_time"} = 3;
	$data_sequence{"load_balance"}{"machine"} = 4;
	
	# record the start time of script
	my ($start_sec,$start_usec) = gettimeofday();
    my $dir = $gOptions{'directory'};
	my $pattern = $gOptions{'pattern'};
   
	# get current user name
	my $user = GetUserInfo;
	
	# remove last result files
	RemoveFile("*.sj_bcp");
	RemoveFile("*.sj_error");
	RemoveFile("*.lb_error");
	RemoveFile("*.lb_bcp");
	
	# init error log file name
	$err_log = $dir.$err_log;
	
	# get log list matched the name pattern
	my $logList = GetLogList($dir,$pattern);
	# parse logs
	ParseLogCollection($dir,$logList);
	# calculate total run time
	CalcRunTime($start_sec,$start_usec);
	return 0;
}
