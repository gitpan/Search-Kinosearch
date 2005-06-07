#!/usr/bin/perl

use lib 'lib';

use strict;
use warnings;

use Test::More tests => 5;
use Test::Exception;
use File::Spec;
use File::Path qw( rmtree );
use Fcntl;
use File::Temp 'tempdir';

my $mainpath = File::Spec->catdir('t', 'kindex');
$mainpath = File::Spec->rel2abs($mainpath);

sub cleanup {
    print "Cleaning up...\n";
    rmtree($mainpath);
}

my $safety = 0;

END {
    exit if $safety;
    &cleanup;
}

&cleanup;

if (-e 'kindex') {
    $safety = 1;
    die "'kindex' already exists";
}

use Search::Kinosearch::Kindexer;
use Search::Kinosearch::Kindex;
use Search::Kinosearch::KSearch;

my $kindexer = Search::Kinosearch::Kindexer->new( 
    -mode => 'overwrite',
    -stoplist => {},
    -mainpath => $mainpath,
);

$kindexer->define_field( 
    -name => 'content',    
    -tokenize,
);

my @docs = (
    'x',
    'y',
    'z',
    'x a',
    'x a b',
    'x a b c',
    'x a b c d',
);
    
for (@docs) {
    my $doc = $kindexer->new_doc($_);
    $doc->set_field( content => $_ );
    $kindexer->add_doc($doc);
}

$kindexer->generate;
$kindexer->write_kindex;
undef $kindexer;

### Add more documents 
&add_to_kindex(4, $_) for (40 .. 42);

my $kindex = Search::Kinosearch::Kindex->new( -mainpath => $mainpath ); 
is($#{ $kindex->{subkindexes} }, 3, 
    "-optimization level 4 should add one subkindex per run");
undef $kindex;
    
&add_to_kindex(1, (10 .. 19));
$kindex = Search::Kinosearch::Kindex->new( -mainpath => $mainpath ); 
is($#{ $kindex->{subkindexes} }, 0, 
    "-optimization level 1 should consolidate subkindexes down to 1");
undef $kindex;

&add_to_kindex(2, $_) for ( 20 .. 22 );

$kindex = Search::Kinosearch::Kindex->new( -mainpath => $mainpath ); 
is($#{ $kindex->{subkindexes} }, 1, 
    "-optimization level 2 should produce no more than 2 subkindexes");
undef $kindex;

&add_to_kindex(3, $_) for ( 30 .. 35 );

$kindex = Search::Kinosearch::Kindex->new( -mainpath => $mainpath ); 
cmp_ok($#{ $kindex->{subkindexes} }, '>', 0, 
    "-optimization level 3 should add subkindexes when there isn't too " .
    "much material");
undef $kindex;

&add_to_kindex(3, $_) for ( 35 .. 39 );
&add_to_kindex(3, (300 .. 399));

$kindex = Search::Kinosearch::Kindex->new( -mainpath => $mainpath ); 
cmp_ok($#{ $kindex->{subkindexes} }, '<', 10, 
    "-optimization level 3 should consolidate when given a lot of material");
undef $kindex;

sub add_to_kindex {
    my $optimization = shift;
    my @content = @_;
    my $kindexer = Search::Kinosearch::Kindexer->new(
        -stoplist      => {},
        -mainpath      => $mainpath,
        -mode          => 'update',
        -optimization  => $optimization,
#        -verbosity     => 1,
        );
    for (@content) {
        my $doc = $kindexer->new_doc($_);
        $doc->set_field( content => $_ );
        $kindexer->add_doc($doc);
    }
    $kindexer->generate;
    $kindexer->write_kindex;
}
    
