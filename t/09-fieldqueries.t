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
    -enable_datetime,
    );

$kindexer->define_field( 
    -name => 'single', 
    );
$kindexer->define_field( 
    -name => 'many',
    -tokenize => 1,
    );

my %docs;
for my $num (0 .. 9) {
    $docs{$num} = '';
    $docs{$num} .= "$_ " for (0 .. $num);
}

$docs{8} = join ' ', (0 ..7, 9);

while (my ($single, $many) = each %docs) {
    my $doc = $kindexer->new_doc( $single );
    $doc->set_field( single  => $single );
    $doc->set_field( many    => $many   );
    $kindexer->add_doc( $doc );
}
$kindexer->generate;
$kindexer->write_kindex;
undef $kindexer;

my $kindex = Search::Kinosearch::Kindex->new;

### Sanity check.
my $ksearch = Search::Kinosearch::KSearch->new( -kindex => $kindex);
my $quob = Search::Kinosearch::Query->new( -string => '0' );
$ksearch->add_query($quob);
my $status = $ksearch->process;
is($status->{num_hits}, 10, "Sanity check");

### Single field query.
$ksearch = Search::Kinosearch::KSearch->new( -kindex => $kindex);
$quob = Search::Kinosearch::Query->new(
    -string => '5', 
    -fields => {
        single => 1,
        }, 
        );
$ksearch->add_query($quob);
$status = $ksearch->process;
is($status->{num_hits}, 1, "Limit queries to a single field");

### Field weights
$ksearch = Search::Kinosearch::KSearch->new( -kindex => $kindex);
$quob = Search::Kinosearch::Query->new(
    -string => '8', 
    -fields => {
        single => .001,
        many   => 1000,
        }, 
        );
$ksearch->add_query($quob);
$status = $ksearch->process;
my @got; while (my $result = $ksearch->fetch_hit_hashref) {
    push @got, $result->{single};
}
is_deeply(\@got, [ 9, 8 ] , "Weight fields");


    
    