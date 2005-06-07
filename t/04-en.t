#!/usr/bin/perl

use lib 'lib';

use strict;
use warnings;

use Test::More tests => 6;
use Test::Exception;

### Note: Many of the functions in Search::Kinosearch::Lingua::En end up 
### being tested via other modules' tests.

use_ok('Search::Kinosearch::Lingua::En');

dies_ok { my $death = Search::Kinosearch::Lingua::En->new(); } 
    "Calling a constructor for Search::Kinosearch::Lingua should fail...";

my $en = {};
bless $en, 'Search::Kinosearch::Lingua::En';
$en->{tokenreg} = $Search::Kinosearch::Lingua::En::tokenreg;

my ($tokenized, $positions) = $en->tokenize(
    'three blind mices.');
is_deeply($tokenized, [ qw(three blind mices) ], 
    "The default tokenizer should return what we expect...");
is_deeply($positions, [ qw(0 6 12) ], 
    "The default tokenizer should get the positions right...");

my $stemmed = $en->stem($tokenized);
is_deeply($stemmed, [ qw(three blind mice) ], 
    "The stemmer should return what we expect...");

ok(("stuff" =~ /^$Search::Kinosearch::Lingua::En::tokenreg$/ 
    and "don't" =~ /^$Search::Kinosearch::Lingua::En::tokenreg$/ 
    and 'x x' !~ /^$Search::Kinosearch::Lingua::En::tokenreg$/), 
    "\$tokenreg behaves as we expect...");