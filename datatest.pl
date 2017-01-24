#!/usr/local/bin/perl5 -w
#
# A tool to make fake OA testing data
#############################################################################

# time schedule to run OA testing

# 3 times for 2 days

# March 8th 10:00 AM 50 new QA fake file 9999900001-9999900020
# March 8th 10:30 AM 50 new QA fake file 9999900021-9999900220
# March 8th 11:00 AM 50 new QA fake file 9999900221-9999902220

# March 9th 10:00 AM 50 new QA fake file 9999910001-9999910020
# March 9th 10:30 AM 50 new QA fake file 9999910021-9999910220
# March 9th 11:00 AM 50 new QA fake file 9999910221-9999912220

use lib (
    './perllib',
);
use lib qw ( /usr/pkgs/Export/US/SunOS5.8_sun4/lib/perl5/fis );

$ENV{SYBASE_OCS}="OCS-12_5";
$ENV{SYBASE}="/usr/pkgs/sybase";
$ENV{LD_LIBRARY_PATH}="/usr/pkgs/sybase/OCS-12_5/lib:/usr/pkgs/sybase/lib";

my $sqsh="/usr/pkgs/sybase/OCS-12_5/bin/sqsh";

use strict;
use DBI;
use warnings FATAL => 'all';
use Fatal qw( open close );
use Time::Local qw( timelocal );
use File::Basename;
use MIME::Lite;
use Data::Dumper;
use Logger;							# for log functions
use Time::Local qw( timelocal );
use Date::Manip;
use Getopt::EvaP;
use POSIX      qw(strftime);

use constant ME         => File::Basename::basename($0);
use constant NOW        => strftime('%Y%m%d.%H%M%S', localtime);

# debug log
my $working_folder = '/usr/pkgs/ops/lhua/OA/code/';  # a hard code code root location
my $debug_file = $working_folder.'log/oatest.log';
my $log = new Logger::Logger ( $debug_file, 0 ) or die "Can't create object";

# database configuration
my $db_cfg = {
	'db_dmo_server'=>'',
	'db_rack_server'=>'',
	'db_cmo_name'=>'',
	'db_rack_name'=>'',
	'db_user'=>'',
	'db_password'=>'',
};

my $directories = {
	'dir_faked'=>'',
	'dir_source'=>'',
	'dir_temp'=>'',
	'dir_incoming'=>'',	
};

my $test_data = {
	'file_name_zip'=>'',
};

my $fake_record_insert_template = "<env:ContentItem action=\"Insert\"><env:Data xsi:type=\"OrganizationDataItem\"><Organization entityCreatedDate=\"fake_dateT05:45:43\" entityModifiedDate=\"fake_dateT05:45:44\" isOrganizationVerified=\"false\" isOrganizationManaged=\"false\" isOrganizationFinancialLocalBranch=\"false\" isOrganizationFinancialForeignBranch=\"false\" isOrganizationOtherBranchandDivision=\"false\"><OrganizationId>fake_perm_id</OrganizationId><AdminStatus effectiveFrom=\"fake_dateT05:45:43\">Published</AdminStatus><OrganizationName effectiveFrom=\"fake_dateT05:00:00\" organizationNameTypeCode=\"LNG\" languageId=\"505062\" organizationNameLocalNormalized=\"EJV QA Faked Orgnization\">EJV QA Faked Orgnization</OrganizationName><OrganizationName effectiveFrom=\"fake_dateT05:00:00\" organizationNameTypeCode=\"SHT\" languageId=\"505062\">EJV QA</OrganizationName><IsPublicFlag>false</IsPublicFlag><OrganizationTypeCode>GVT</OrganizationTypeCode><OrganizationStatusCode>Act</OrganizationStatusCode><OrganizationProviderTypeCode>1</OrganizationProviderTypeCode><OrganizationJurisdictionOfIncorporationTypeCode>477</OrganizationJurisdictionOfIncorporationTypeCode><OrganizationJurisdictionOfIncorporation>TEXAS</OrganizationJurisdictionOfIncorporation><OrganizationSubtypeCode>GVT</OrganizationSubtypeCode></Organization></env:Data></env:ContentItem>";

my $file_end = "</env:Body></env:ContentEnvelope>";

HelpMessage() unless defined($ARGV[0]);
HelpMessage() unless defined($ARGV[1]);
HelpMessage() unless defined($ARGV[2]);
excute_test();
# print help if no option
sub HelpMessage {
	my $error_msg = shift;
	my $msg = <<'MSG';

-== About ==-
Performance testing on OA

-== Usage ==-
./oatest.pl 9999900001 20 old

-== Option Definition ==-
9999900001 : which perm id to start
20 : how many perm id we'd like to fake
old : test on old OA cycle or new OA cycle [old|new]

MSG
	print $msg;
	exit;
}


sub excute_test
{
	my $sec_begin = time; # begin time
	
	# job parameters
	my $perm_id_base = $ARGV[0]; 	# from which faked perm id to start. like 9999910001
	my $perm_ids_count = $ARGV[1]; 	# how many perm_ids to run, like 20
	my $mode = $ARGV[2]; 			# running on old OA cycle or new oa cycle
	
	$log->debug_message("[INFO] Start testing for faked perm id $ARGV[0] to ".(int($ARGV[0])+int($ARGV[1]))." on $ARGV[2] OA cycle.");
	
	my $date_local = strftime('%Y-%m-%d',localtime); # today's date, like 2012-03-07
	
	# 1: get latest fetch_ok file from DB
	$log->debug_message("[INFO] Step 1, get latest fetch_ok file from database.");
	
	my $dbh = DBI->connect("dbi:Sybase:server=$db_cfg->{db_dmo_server};'oa_feed'","$db_cfg->{db_user}","$db_cfg->{db_password}")
		or die "Cannot connect to server: $DBI::errstr\n";
	$log->debug_message("[INFO] Conneted to the DB.");
	my @DB_reuslts = @{$dbh->selectcol_arrayref("Select max(name) from oa_feed..files where partition_id=18 and status=2")};
	if(!$DB_reuslts[0])
	{
		$log->debug_message("[INFO] Did not got any record for testing, Quit testing.");
		my $rc = $dbh->disconnect;
		return 0;
	}
	$log->debug_message("[INFO] Got DB records @DB_reuslts.");
	my $rv = $dbh->disconnect;
	$test_data->{file_name_zip} = $DB_reuslts[0];	
	
	# 2: fetch the file and make a fake one for QA.
	$log->debug_message("[INFO] Step 2, fetch file and make a fake file.");
	# 2.1 fetch the file
	my $copy_oa_file = "scp ops\@hsfiscoll1:$directories->{dir_incoming}/$test_data->{file_name_zip} $directories->{dir_source}";
	$log->debug_message("[INFO] $copy_oa_file");
	system("$copy_oa_file");
	
	# 2.2 unzip the file
	system( "rm -rf $directories->{dir_temp}/*.*") == 0 or die "Failed to clean $directories->{dir_temp}";
	system( "unzip $directories->{dir_source}/$test_data->{file_name_zip} -d $directories->{dir_temp}") == 0 or die "Failed to unzip $test_data->{file_name_zip}";
	
	# 2.3 fake data injection
	my $file_name_unzip = `find $directories->{dir_temp} -name EntityMaster.ThomsonReuters.Organization*`;
	chomp( $file_name_unzip );
	$log->debug_message("[INFO] Unziped file name is : $file_name_unzip");
	open( my $fh_read, '<', $file_name_unzip ) or die "failed to open $file_name_unzip \n";
	open( my $fh_write, '>>', $file_name_unzip.".temp" ) or die "failed to write $file_name_unzip.'.temp' \n";
	
	my $new_hdr;
	my $line = <$fh_read>; # get the first line as head;
	print $fh_write $line;
	
	chomp( $line );
	while($line = <$fh_read>)
	{
		chomp($line);
		if($line !~ m/<\/env:Body><\/env:ContentEnvelope>/)
		{
			print $fh_write $line;
			$log->debug_message("[DEBUG] it's not the last line, then fetch next line.");
			next;
		}
		
		my $new_line=$line;
		
		my $count=0;	# perm id add up counter
		while($count++ < $perm_ids_count)
		{
			my $fake_record = $fake_record_insert_template;
			$fake_record =~ s/fake_perm_id/$perm_id_base/;
			$perm_id_base++;
			$fake_record =~ s/fake_date/$date_local/g;
			
			my $end_content_length = length($file_end);
			
			my $new_line_length;
			if($count eq 1) 
			{
				# when get the first time, elimilate the newline from length
				$new_line_length = length($new_line)-1;
			}
			else
			{
				$new_line_length = length($new_line);
			}

			$new_line = substr($new_line,0,($new_line_length-$end_content_length)).$fake_record.$file_end;
		}
				
		print $fh_write $new_line;
		print $fh_write '';
		print $fh_write '';
	}
	
	close( $fh_write );
	close( $fh_read );
	
	$file_name_unzip =~ s{^.*/}{};
	
	system("mv $directories->{dir_temp}/$file_name_unzip.temp $directories->{dir_faked}/$file_name_unzip")  == 0 or die "Failed copy file to $directories->{dir_faked}/$file_name_unzip";
	
	system("zip -j $directories->{dir_faked}/$file_name_unzip.zip $directories->{dir_faked}/$file_name_unzip") == 0 or die "Failed to make zip file $directories->{dir_faked}/$file_name_unzip.zip";
	
	system("rm $directories->{dir_faked}/$file_name_unzip");
	
	# 3: load file and monitoring
	$log->debug_message("[INFO] Step 3, upload faked file to replace original file from ftp.");
	my $check_times = 0; 
	my $max_check_times_dmo = 500; 	# set try 300 times for each round of status checking. 
	
	# status in database
	my $status_fetch_ok = 2;			# 2: fetch OK
	my $status_parsing = 3;				# 3: parsing
	my $status_parse_error_4 = 4; 		# 4/5: parse error
	my $status_parse_error_5 = 5; 		# 4/5: parse error
	my $status_parse_ok = 6;			# 6: parse OK
	my $status_transforming = 7;		# 7: transforming
	my $status_transform_ok = 8;		# 8: transform OK
	
	# timestamp of each file status (not use)
	my $sec_fetch_ok = 0;				# fetch OK
	my $sec_parsing = 0;				# parsing
	my $sec_parse_ok = 0;				# parse OK
	my $sec_transforming = 0;			# transforming
	my $sec_transform_ok = 0;			# transform OK
	my $sec_rack_ok = 0;				# rack OK
	
	# sleep time
	my $sleep_fetch_ok = 60;			# fetch OK
	my $sleep_parsing = 10;				# parsing
	my $sleep_parse_ok = 60;			# parse OK
	my $sleep_transforming = 10;		# transforming
	my $sleep_transform_ok = 60;		# transform OK
	
	if($ARGV[2] eq 'new')
	{
		#sleep 3 seconds in continues mode
		$sleep_fetch_ok = 3;			# fetch OK
		$sleep_parsing = 3;				# parsing
		$sleep_parse_ok = 3;			# parse OK
		$sleep_transforming = 3;		# transforming
		$sleep_transform_ok = 3;		# transform OK
	}
	
	
	# 3.1 load faked file
	my $upload_faked_oa_file = "scp $directories->{dir_faked}/$file_name_unzip.zip ops\@hsfiscoll1:$directories->{dir_incoming}/$test_data->{file_name_zip} ";
	
	system($upload_faked_oa_file);
	$log->debug_message("[INFO] Faked file upload to OA cycle successful.");
	
	
	# 3.2 monitoring file to dmo database
	$log->debug_message("[INFO] Step 3.x, monitoring fake files.");
	
	
	my $dbh1 = DBI->connect("dbi:Sybase:server=$db_cfg->{db_dmo_server};'oa_feed'","$db_cfg->{db_user}","$db_cfg->{db_password}")
		or die "Cannot connect to server: $DBI::errstr\n";
	$log->debug_message("[INFO] Conneted to the dmo database.");
	
	while($check_times < $max_check_times_dmo)
	{
		my @DB_reuslts1 = @{$dbh1->selectcol_arrayref("Select status from oa_feed..files where name = '$test_data->{file_name_zip}'")};
		if(!$DB_reuslts1[0])
		{
			$log->debug_message("[INFO] Got unexpected results, Disconnected DB!");
			my $rc = $dbh1->disconnect;
			last;
		}
		$check_times++;
		
		$log->debug_message("[INFO] Rest checking times: $check_times/$max_check_times_dmo");
		
		if($DB_reuslts1[0] eq $status_fetch_ok)
		{
			if($sec_fetch_ok eq 0)
			{
				$sec_fetch_ok = time;
			}
			$log->debug_message("[INFO] Test file status is ==== Fetch OK ====.");
			sleep ($sleep_fetch_ok);
			next;
		}
		elsif($DB_reuslts1[0] eq $status_parsing)
		{
			if($sec_parsing eq 0)
			{
				$sec_parsing = time;
			}
			$log->debug_message("[INFO] Test file status is ==== Parsing ====.");
			sleep ($sleep_parsing);
			next;
		}
		elsif($DB_reuslts1[0] eq $status_parse_error_4)
		{
			$log->debug_message("[INFO] Test file status is ==== Parsing ERROR 4 ====.","DEBUG");
			my $rc = $dbh1->disconnect;
			return 0;
		}
		elsif($DB_reuslts1[0] eq $status_parse_error_5)
		{
			$log->debug_message("[INFO] Test file status is ==== Parsing ERROR 5 ====.","DEBUG");
			my $rc = $dbh1->disconnect;
			return 0;
		}
		elsif($DB_reuslts1[0] eq $status_parse_ok)
		{
			if($sec_parse_ok eq 0)
			{
				$sec_parse_ok = time;
			}
			$log->debug_message("[INFO] Test file status is ==== parse OK ====.");
			sleep ($sleep_parse_ok);
			next;
		}
		elsif($DB_reuslts1[0] eq $status_transforming)
		{
			if($sec_transforming eq 0)
			{
				$sec_transforming = time;
			}
			$log->debug_message("[INFO] Test file status is ==== Transforming ====.");
			sleep ($sleep_transforming);
			next;
		}
		elsif($DB_reuslts1[0] eq $status_transform_ok)
		{
			if($sec_transform_ok eq 0)
			{
				$sec_transform_ok = time;
			}
			$log->debug_message("[INFO] Test file status is ==== Transform OK ====.");
			sleep ($sleep_transform_ok);
			$perm_id_base = $ARGV[0]; # reset perm id base
			last;
		}
	}
	
	# check last change date in dmo database
	while($check_times < $max_check_times_dmo)
	{
		my @DB_reuslts1 = @{$dbh1->selectcol_arrayref("Select last_chg_dt from dmo_entity..entity where entity_permid = $perm_id_base")};
		if(!$DB_reuslts1[0])
		{
			$log->debug_message("[INFO] Still do not get expected records in DMO database");
			sleep(60);
			next;
		}
		else
		{
			$log->debug_message("[INFO] last update time in dmo is $DB_reuslts1[0]");
			my $rc1 = $dbh1->disconnect;
			last;
		}
	}

	my $rv1 = $dbh1->disconnect;	
	
	# 3.3 monitoring from dmo to RACK
	my $dbh2 = DBI->connect("dbi:Sybase:server=$db_cfg->{db_rack_server};'data_entity'","$db_cfg->{db_user}","$db_cfg->{db_password}")
		or die "Cannot connect to server: $DBI::errstr\n";
	$log->debug_message("[INFO] Conneted to the DB.");
	
	$check_times = 0;	# reset check_times to 0
	
	while($check_times < $max_check_times_dmo)
	{
		my @DB_reuslts2 = @{$dbh2->selectcol_arrayref("Select last_chg_dt from data_entity..entity where entity_permid = $perm_id_base")};		
		$check_times++;
		if(!$DB_reuslts2[0])
		{
			$log->debug_message("[INFO] Data in rack is not ready! sleep one minutes.");
			$log->debug_message("[INFO] Checking times: $check_times/$max_check_times_dmo");
			sleep(60);
			next;
		}
		else
		{
			$log->debug_message("[INFO] data is ready in RACK time: $DB_reuslts2[0]");
			$sec_rack_ok = time;
			my $rc2 = $dbh2->disconnect;
			last;
		}
	}
	
	my $rv2 = $dbh2->disconnect;
	
	my $sec_end = time; # end time
	$log->debug_message("[INFO] test is done, total time: ".($sec_end - $sec_begin)." seconds.");
	# 4: done and analysis
	$log->debug_message("==========================================================");
	$log->debug_message("===================Testing Statistics=====================");
	$log->debug_message("==========================================================");
	$log->debug_message("total records in testing file: not known ");
	$log->debug_message("total time in testing : ".($sec_end - $sec_begin)." seconds.");
	$log->debug_message("from fetch_ok to parsing: ".($sec_parsing - $sec_fetch_ok)." seconds");
	$log->debug_message("from parsing to parse_ok: ".($sec_parse_ok - $sec_parsing)." seconds");
	$log->debug_message("from parse_ok to transforming: ".($sec_transforming - $sec_parse_ok)." seconds");
	$log->debug_message("from transforming to transform_ok: ".($sec_transform_ok - $sec_transforming)." seconds");
	$log->debug_message("from transform_ok to rack_ok: ".($sec_rack_ok - $sec_transform_ok)." seconds");
	$log->debug_message("==========================================================");
	return 1;
}









