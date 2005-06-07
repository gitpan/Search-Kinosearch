#!/usr/bin/perl

use lib 'lib';

use strict;
use warnings;

use Test::More tests => 14; 
use Test::Exception;
use File::Spec;
require(File::Spec->catfile('t','common_tests.pl'));

use_ok('Search::Kinosearch');

dies_ok { my $death = Search::Kinosearch->new(); } 
    "Calling a constructor for Search::Kinosearch should fail...";

### We're going to create a Search::Kinosearch object, 
### even though Search::Kinosearch is an abstract base class, 
### so that we can test the methods other classes will inherit.

my $kinosearch = {};

bless $kinosearch, 'Search::Kinosearch';
die "Object isn't a Search::Kinosearch" 
    unless (ref($kinosearch) eq 'Search::Kinosearch');

&test_language_specific_import($kinosearch);