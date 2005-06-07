#!/usr/bin/perl

use lib 'lib';

use strict;
use warnings;

use Test::More tests => 8;
use Test::Exception;

use_ok('Search::Kinosearch::Lingua');

dies_ok { my $death = Search::Kinosearch::Lingua->new(); } 
    "Calling a constructor for Search::Kinosearch::Lingua should fail...";

### We're going to create a Search::Kinosearch::Lingua object, 
### even though Search::Kinosearch::Lingua is an abstract base class, 
### so that we can test the methods other classes will inherit.
my $lingua = {};
bless $lingua, 'Search::Kinosearch::Lingua';

$lingua->{tokenreg} = $Search::Kinosearch::Lingua::tokenreg;

can_ok($lingua, 'tokenize');

my ($tokenized, $positions) = $lingua->tokenize('three blind mice');
is_deeply($tokenized, [ qw(three blind mice) ], 
    "The default tokenizer should return what we expect...");

is_deeply($positions, [ qw(0 6 12) ], 
    "The default tokenizer should get the positions right...");

can_ok($lingua, 'stem');

my $stemmed = $lingua->stem($tokenized);
is_deeply($stemmed, [ qw(three blind mice) ], 
    "The default stemmer is a no-op...");

ok(("stuff" =~ /^$Search::Kinosearch::Lingua::tokenreg$/ 
    and "don't" !~ /^$Search::Kinosearch::Lingua::tokenreg$/ 
    and 'x x' !~ /^$Search::Kinosearch::Lingua::tokenreg$/), 
    "\$tokenreg behaves as we expect...");