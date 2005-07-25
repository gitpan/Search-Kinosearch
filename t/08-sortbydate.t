#!/usr/bin/perl

use lib 'lib';

use strict;
use warnings;

use Test::More 'no_plan'; 
use Test::Exception;
use File::Spec;
use File::Path qw( rmtree );


sub cleanup {
    print "Cleaning up...\n";
    rmtree("kindex");
    rmtree( File::Spec->catdir('sample_files','kindex'));
}

my $safety = 0;

END {
    exit if $safety;
    &cleanup;
}

if (-e 'kindex') {
    $safety = 1;
    die "'kindex' already exists";
}


use Search::Kinosearch::Kindexer;
use Search::Kinosearch::KSearch;

my $kindexer = Search::Kinosearch::Kindexer->new(
    -stoplist => {},
    -enable_datetime => 1,
    );

$kindexer->define_field( 
    -name => 'content',
    -tokenize => 1,
    );

my %docs = (
    'a a b'         => [ 0,4,1,25,61,61], # Apr 1, 0, 25:61:61
    'a a a b'       => [ 777,7,7,5,5,5 ], # Jul 7, 777, 5:05:05
    'a b'           => [ 2001,1,1,0,0,0 ], # Jan 1, 2001, 00:00:00
    );

while (my ($content, $date_arr) = each %docs) {
    my $doc = $kindexer->new_doc( $content );
    $doc->set_field( content => $content );
    $doc->set_datetime( @$date_arr );
    $kindexer->add_doc( $doc );
}
$kindexer->generate;
$kindexer->write_kindex;
undef $kindexer;

### Sanity check.
my $ksearch = Search::Kinosearch::KSearch->new( -stoplist => {});
my $quob = Search::Kinosearch::Query->new(
    -string => 'a', 
    -tokenize => 1, 
    );
$ksearch->add_query($quob);
$ksearch->process;
my @expected = ('a a a b', 'a a b', 'a b');
my @got;
while (my $result = $ksearch->fetch_hit_hashref) {
    push @got, $result->{content};    
}
is_deeply(\@got, \@expected, "Proper order by relevance");

    
### Verify that date range queries work.
@got = ();
@expected = ('a a a b');
$ksearch = Search::Kinosearch::KSearch->new( -stoplist => {});
$quob = Search::Kinosearch::Query->new(
    -string => 'a', 
    -tokenize => 1, 
    -min_date => [500,0,0,0,0,0],
    -max_date => [1900,0,0,0,0,0],
    );
$ksearch->add_query( $quob );
$ksearch->process;
while (my $result = $ksearch->fetch_hit_hashref) {
    push @got, $result->{content};    
}
is_deeply(\@got, \@expected, "Date range works");
    
### Verify sort by datetime.
@got = ();
@expected = ('a b', 'a a a b', 'a a b');
$ksearch = Search::Kinosearch::KSearch->new( 
    -stoplist => {},
    -sort_by  => 'datetime',
    );
$quob = Search::Kinosearch::Query->new(
    -string => 'a', 
    -tokenize => 1, 
    );
$ksearch->add_query( $quob );
$ksearch->process;
while (my $result = $ksearch->fetch_hit_hashref) {
    push @got, $result->{content};    
}
is_deeply(\@got, \@expected, "Sort by datetime works");

    