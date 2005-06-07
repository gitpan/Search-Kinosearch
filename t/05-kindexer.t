#!/usr/bin/perl

use lib 'lib';

use strict;
use warnings;

use Test::More 'no_plan';
use Test::Exception;
use File::Spec;
use File::Path qw( rmtree );
use Fcntl;
use File::Temp 'tempdir';
use Digest::MD5 'md5_hex';

sub cleanup {
    print "Cleaning up...\n";
    rmtree("kindex");
    rmtree( File::Spec->catdir('sample_files','kindex'));
}

BEGIN {
    require( File::Spec->catfile('t','common_tests.pl')) 
        or die "couldn't find file";
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

use_ok('Search::Kinosearch::Kindexer');
use_ok('Search::Kinosearch::KSearch');
use_ok('Search::Kinosearch::Query');

dies_ok { my $bad = Search::Kinosearch::Kindexer->new(-death => 'bad') }
    "An illegal parameter should cause new() to throw an exception...";
    
### STEP 1: Create a Kindexer object.
my $kindexer = Search::Kinosearch::Kindexer->new( );
isa_ok ($kindexer, 'Search::Kinosearch::Kindexer', 
    "The constructor should produce the right kind of object..."); 

can_ok($kindexer, 'define_field');

my $current_directory = File::Spec->curdir;
$current_directory = File::Spec->rel2abs($current_directory);
my $default_mainpath = File::Spec->catdir($current_directory, 'kindex');
$default_mainpath = File::Spec->rel2abs($default_mainpath);

### A battery of tests for functions loaded via Search::Kinosearch
&test_language_specific_import($kindexer);

undef $kindexer;
    
$kindexer = Search::Kinosearch::Kindexer->new();
            
dies_ok { $kindexer->define_field(-death => 'bad') }
    "An illegal parameter should cause define_field to throw an " .
    "exception...";
dies_ok { $kindexer->define_field(-name => 'score') }
    "Trying to define a field called 'score' should throw an " .
    "exception...";
dies_ok { $kindexer->define_field(-name => 'doc_id') }
    "Trying to define a field called 'doc_id' should throw an " .
    "exception...";
$kindexer->define_field(-name => 'foo');
dies_ok { $kindexer->define_field(-name => 'foo') }
    "Trying to define the same field twice should throw an exception...";

my $old_scorable_fields = @{ $kindexer->{fields_score} };
my $old_storable_fields = @{ $kindexer->{fields_store} };
$kindexer->define_field(
    -name => 'tester',
);
my $new_scorable_fields = @{ $kindexer->{fields_score} };
my $new_storable_fields = @{ $kindexer->{fields_store} };
is(($old_scorable_fields + 1), $new_scorable_fields, 
    "Fields should be scorable by default...");
is(($old_storable_fields + 1), $new_storable_fields, 
    "Fields should be storable by default...");

undef $kindexer;

$kindexer = Search::Kinosearch::Kindexer->new();

$kindexer->define_field( -name => 'stuff' );

can_ok($kindexer, 'new_doc');

my $document = $kindexer->new_doc('stuffity_stuff');

can_ok($document, 'set_field');

$document->set_field( stuff => 'storf!' );

can_ok($kindexer, 'add_doc');

$kindexer->add_doc( $document );

dies_ok { $kindexer->define_field(-name => 'death') }
    "Attempting to define a field after indexing has started " .
    "should cause an exception...";

undef $kindexer;

$kindexer = Search::Kinosearch::Kindexer->new();

$kindexer->define_field( 
    -name => 'lowercase',    
    -lowercase => 1,
    );
$kindexer->define_field( 
    -name => 'lowercase_tokenize',    
    -lowercase => 1,
    -tokenize => 1,
    );
$kindexer->define_field( 
    -name => 'lowercase_tokenize_stem',    
    -lowercase => 1,
    -tokenize => 1,
    );


my $count = 0;
my $moment_in_time = time();
my $test_val = '';

my @pomes = (
    'thrEe blind mices',
    'thrEe blind mices',
    'see how they ruNs',
    'see how they ruNs',
    );

for my $pome (@pomes) {
    my $doc = $kindexer->new_doc($pome);
    $doc->set_field( lowercase => $pome );
    $doc->set_field( lowercase_tokenize => $pome );
    $doc->set_field( lowercase_tokenize_stem => $pome );
    $kindexer->add_doc( $doc );
}

$kindexer->generate;
$kindexer->write_kindex;
    
undef $kindexer;


my $ksearch = Search::Kinosearch::KSearch->new;
my $query = Search::Kinosearch::Query->new(
    -string => 'three blind mices',
    -fields => { lowercase => 1 },
    );
$ksearch->add_query($query);
my $status = $ksearch->process;
is($status->{num_hits}, 1, 'Adding the same document twice should only ' .
    'produce one doc in the index');


$ksearch = Search::Kinosearch::KSearch->new;
$query = Search::Kinosearch::Query->new(
    -string => 'thrEe blind mices',
    -fields => { lowercase => 1 },
    );
$ksearch->add_query($query);
$status = $ksearch->process;
is($status->{num_hits}, 0, 'lowercasing works');

$ksearch = Search::Kinosearch::KSearch->new;
$query = Search::Kinosearch::Query->new(
    -string => 'runs',
    -fields => { lowercase_tokenize => 1 },
    );
$ksearch->add_query($query);
$status = $ksearch->process;
is($status->{num_hits}, 1, 'tokenizing works, part 1');

$ksearch = Search::Kinosearch::KSearch->new;
$query = Search::Kinosearch::Query->new(
    -string => 'see how they runs',
    -fields => { lowercase_tokenize => 1 },
    );
$ksearch->add_query($query);
$status = $ksearch->process;
is($status->{num_hits}, 0, 'tokenizing works, part 2');

$ksearch = Search::Kinosearch::KSearch->new;
$query = Search::Kinosearch::Query->new(
    -string => 'run',
    -fields => { lowercase_tokenize_stem => 1 },
    );
$ksearch->add_query($query);
$status = $ksearch->process;
is($status->{num_hits}, 0, 'stemming works');
