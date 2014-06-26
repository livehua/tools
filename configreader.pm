# read configuration files
sub Configuration{
	my $config = shift;
	die "Need config file." if (! defined $config);
	
	my (%dict,$this);
	
	#Read Config File#
	open FILE,"$config" or die $!;
	$this = \%dict;
	while(<FILE>){
		chomp;
		if(/^\s*;/ or /^\s*$/){
			next;
		}
		elsif(/^\s*\[(\w+)\]\s*$/){
			if(exists $dict{$1}){
				$this = $dict{$1};
			}
			else{
				$dict{$1} = {};
				$this = $dict{$1};
			}        
		}
		elsif(/^\s*(.+)\s*=\s*(.+)\s*$/){
			#($param,$value) = ($1,$2); #split '=',$_;
			$this->{$1}=$2;
		}
		else{
			die "Line format error: $_";
		}
	}

	return \%dict;
}

1;
