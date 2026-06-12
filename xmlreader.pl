#!/usr/local/bin/perl5 -w
#
# XML file reader and parser
# Supports lightweight regex parsing (no external deps) or XML::Simple if available
#############################################################################

use strict;
use warnings FATAL => 'all';
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use Data::Dumper;

use constant ME => File::Basename::basename($0);

my %gOptions;
my $use_xml_simple = 0;

# Try to load XML::Simple if available
eval {
    require XML::Simple;
    XML::Simple->import();
    $use_xml_simple = 1;
};

HelpMessage() unless @ARGV;
Setup(ME);
Main();

######################################################################
#                Functions
######################################################################

####################
# Function:	usage
####################
sub Usage {
    my ($pgm) = @_;
    print <<"END_USAGE";
Usage: $pgm [options] -f xmlfile
Options:
	-file		XML file to parse (required)
	-path		Node path to query, dot-separated (e.g., "server.host")
	-flat		Output flattened key=value format
	-dump		Dump full Perl data structure
	-help		Show this help message

Examples:
	$pgm -f config.xml
	$pgm -f config.xml -path database.connection
	$pgm -f config.xml -flat
	$pgm -f config.xml -dump

END_USAGE
    exit(1);
}

####################
# Function:	Setup
####################
sub Setup {
    my ($pgm) = @_;

    GetOptions(
        \%gOptions, 'file=s', 'path=s', 'flat', 'dump', 'help',
    ) or Usage($pgm);

    Usage($pgm) if ( $gOptions{'help'} );
    Usage($pgm) if ( ! defined( $gOptions{'file'} ) );
    Usage($pgm) if ( ! -f $gOptions{'file'} );
}

####################
# Function:	ReadXML
#
# Read and parse XML file into Perl data structure
####################
sub ReadXML {
    my ($file) = @_;
    die "Need xml file." if (! defined $file);
    die "File not found: $file" if (! -f $file);

    open FILE, "$file" or die $!;
    my $content = '';
    while(<FILE>){
        $content .= $_;
    }
    close FILE;

    if ($use_xml_simple) {
        return XML::Simple::XMLin($content, ForceArray => 0, KeyAttr => []);
    }
    else {
        return ParseXMLLite($content);
    }
}

####################
# Function:	ParseXMLLite
#
# Lightweight XML parser using regex (no external modules)
# Returns nested hash structure
####################
sub ParseXMLLite {
    my ($xml) = @_;
    my $result = {};

    # Remove XML declaration and comments
    $xml =~ s/<\?xml.*?\?>//s;
    $xml =~ s/<!--.*?-->//gs;
    $xml =~ s/<!DOCTYPE.*?>//s;

    # Parse root element and children
    $result = ParseElement($xml);

    return $result;
}

####################
# Function:	ParseElement
#
# Recursively parse XML elements
####################
sub ParseElement {
    my ($xml) = @_;
    my %data;

    # Extract attributes from opening tag if present
    while ($xml =~ /<(\w+)([^>]*)>(.*?)<\/\1>/s) {
        my ($tag, $attrs, $inner) = ($1, $2, $3);
        my $rest = $';

        my %attrs = ParseAttributes($attrs);
        my $value;

        # Check if inner content has child elements
        if ($inner =~ /<\w+/) {
            $value = ParseElement($inner);
            # Merge attributes into the hash
            foreach my $k (keys %attrs) {
                $value->{"_$k"} = $attrs{$k};
            }
        }
        else {
            $inner =~ s/^\s+|\s+$//g;
            if (keys %attrs) {
                $value = { '_content' => $inner };
                foreach my $k (keys %attrs) {
                    $value->{"_$k"} = $attrs{$k};
                }
            }
            else {
                $value = $inner;
            }
        }

        # Handle multiple same-named tags
        if (exists $data{$tag}) {
            if (ref($data{$tag}) eq 'ARRAY') {
                push @{$data{$tag}}, $value;
            }
            else {
                $data{$tag} = [ $data{$tag}, $value ];
            }
        }
        else {
            $data{$tag} = $value;
        }

        $xml = $rest;
    }

    return \%data;
}

####################
# Function:	ParseAttributes
#
# Parse XML attributes into hash
####################
sub ParseAttributes {
    my ($attr_str) = @_;
    my %attrs;

    while ($attr_str =~ /(\w+)=["']([^"']+)["']/g) {
        $attrs{$1} = $2;
    }

    return %attrs;
}

####################
# Function:	GetNode
#
# Query nested hash by dot-separated path
####################
sub GetNode {
    my ($data, $path) = @_;
    return undef if (! defined $data || ! defined $path);

    my @keys = split /\./, $path;
    my $current = $data;

    foreach my $key (@keys) {
        return undef if (! defined $current);

        if (ref($current) eq 'HASH' && exists $current->{$key}) {
            $current = $current->{$key};
        }
        elsif (ref($current) eq 'ARRAY' && $key =~ /^\d+$/ && $key < @$current) {
            $current = $current->[$key];
        }
        else {
            return undef;
        }
    }

    return $current;
}

####################
# Function:	PrintFlat
#
# Print nested hash as flattened key=value pairs
####################
sub PrintFlat {
    my ($data, $prefix) = @_;
    $prefix = '' if (! defined $prefix);

    if (ref($data) eq 'HASH') {
        foreach my $key (sort keys %$data) {
            my $new_prefix = $prefix ? "$prefix.$key" : $key;
            PrintFlat($data->{$key}, $new_prefix);
        }
    }
    elsif (ref($data) eq 'ARRAY') {
        for (my $i = 0; $i < @$data; $i++) {
            my $new_prefix = "$prefix\[$i\]";
            PrintFlat($data->[$i], $new_prefix);
        }
    }
    else {
        print "$prefix=$data\n" if (defined $data && $data ne '');
    }
}

####################
# Function:	PrintValue
#
# Print a value (scalar, hash, or array)
####################
sub PrintValue {
    my ($data) = @_;

    if (ref($data) eq 'HASH') {
        foreach my $key (sort keys %$data) {
            print "$key: ";
            if (ref($data->{$key})) {
                print "\n";
                PrintValue($data->{$key});
            }
            else {
                print $data->{$key} . "\n";
            }
        }
    }
    elsif (ref($data) eq 'ARRAY') {
        foreach my $item (@$data) {
            PrintValue($item);
            print "---\n";
        }
    }
    else {
        print $data . "\n" if (defined $data);
    }
}

####################
# Function:	Main
######################################################################
sub Main {
    my $file = $gOptions{'file'};
    my $path = $gOptions{'path'};

    print "[INFO] Parsing XML file: $file\n";
    print "[INFO] Using XML::Simple\n" if ($use_xml_simple);
    print "[INFO] Using lightweight parser\n" if (! $use_xml_simple);

    my $data = ReadXML($file);

    if (! defined $data || (ref($data) eq 'HASH' && scalar(keys %$data) == 0)) {
        die "[ERROR] Failed to parse XML or empty content\n";
    }

    if ($gOptions{'dump'}) {
        print "\n";
        print Dumper($data);
    }
    elsif ($gOptions{'flat'}) {
        print "\n";
        PrintFlat($data);
    }
    elsif (defined $path) {
        my $node = GetNode($data, $path);
        if (defined $node) {
            print "\n";
            PrintValue($node);
        }
        else {
            die "[ERROR] Path not found: $path\n";
        }
    }
    else {
        print "\n";
        PrintValue($data);
    }

    return 0;
}
