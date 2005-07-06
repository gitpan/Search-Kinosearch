#!/usr/bin/perl

use lib 'lib';

$|++;
use strict;
use warnings;

use Test::More tests => 460;
use Test::Exception;
use File::Spec;
use File::Path qw( rmtree );
use Fcntl;
use File::Temp 'tempdir';

### This is for debugging only.
use Data::Dumper;
$Data::Dumper::Indent = 1;
$Data::Dumper::Sortkeys = \&reversesort; 
sub reversesort {
    my $hashref = shift;
    my @sorted = sort {$b cmp $a} keys %$hashref; 
    return \@sorted;
    my @sortlist = ('qstring', 'prodtype');
    for (keys %$hashref) {
        push @sortlist, $_
            if /\d/;
    }
    for (qw( required negated productions set_size result_set)) {
        push @sortlist, $_
            if exists $hashref->{$_};
    }
    return \@sortlist;
};

my $mainpath = File::Spec->catdir('t', 'kindex');
$mainpath = File::Spec->rel2abs($mainpath);

sub cleanup {
    print "Cleaning up...\n";
    rmtree($mainpath);
}

my $safety = 0;

END {
    exit if $safety;
#    &cleanup;
}

&cleanup;

if (-e 'kindex') {
    $safety = 1;
    die "'kindex' already exists";
}

use_ok('Search::Kinosearch::Kindexer');
use_ok('Search::Kinosearch::Kindex');
use_ok('Search::Kinosearch::KSearch');
use_ok('Search::Kinosearch::Query');

my $kindexer = Search::Kinosearch::Kindexer->new( 
    -mode => 'overwrite',
    -stoplist => {},
    -mainpath => $mainpath,
);

$kindexer->define_field( 
    -name => 'content',    
    -tokenize => 1,
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

my @configs = (
    [ -allow_boolean => 1, -allow_phrases => 1, ],
    [ -allow_boolean => 1, -allow_phrases => 1, ],
    
    [ -allow_boolean => 0, -allow_phrases => 1, ],
    [ -allow_boolean => 0, -allow_phrases => 1, ],
    
    [ -allow_boolean => 1, -allow_phrases => 0, ],
    [ -allow_boolean => 1, -allow_phrases => 0, ],

    [ -allow_boolean => 0, -allow_phrases => 0, ],
    [ -allow_boolean => 0, -allow_phrases => 0, ],
);

my @logical_tests = (
    
    'b'         => [ 3,3,  3,3,  3,3,  3,3 ],
    '(a)'       => [ 4,4,  4,4,  4,4,  4,4 ],
    '"a"'       => [ 4,4,  4,4,  4,4,  4,4 ],
    '"(a)"'     => [ 4,4,  4,4,  4,4,  4,4 ],
    '("a")'     => [ 4,4,  4,4,  4,4,  4,4 ],
 
    'a b'       => [ 4,3,  4,3,  4,3,  4,3 ], 
    'a (b)'     => [ 4,3,  4,3,  4,3,  4,3 ],
    'a "b"'     => [ 4,3,  4,3,  4,3,  4,3 ],
    'a ("b")'   => [ 4,3,  4,3,  4,3,  4,3 ],
    'a "(b)"'   => [ 4,3,  4,3,  4,3,  4,3 ], 

    '(a b)'     => [ 4,3,  4,3,  4,3,  4,3 ],
    '"a b"'     => [ 3,3,  3,3,  4,3,  4,3 ],
    '("a b")'   => [ 3,3,  3,3,  4,3,  4,3 ],
    '"(a b)"'   => [ 3,3,  3,3,  4,3,  4,3 ], 
    
    'a b c'     => [ 4,2,  4,2,  4,2,  4,2 ],
    'a (b c)'   => [ 4,2,  4,2,  4,2,  4,2 ],
    'a "b c"'   => [ 4,2,  4,2,  4,2,  4,2 ],
    'a ("b c")' => [ 4,2,  4,2,  4,2,  4,2 ],
    'a "(b c)"' => [ 4,2,  4,2,  4,2,  4,2 ],
    '"a b c"'   => [ 2,2,  2,2,  4,2,  4,2 ],
   
    '-x'        => [ 5,5,  5,5,  5,5,  5,5 ], # Should really be 0,0.
    'x -c'      => [ 3,3,  5,2,  3,3,  5,2 ],
    'x "-c"'    => [ 5,2,  5,2,  5,2,  5,2 ],
    'x +c'      => [ 2,2,  5,2,  2,2,  5,2 ],
    'x "+c"'    => [ 5,2,  5,2,  5,2,  5,2 ],
    
    '+x +c'    => [ 2,2,  5,2,  2,2,  5,2 ],
    '+x -c'    => [ 3,3,  5,2,  3,3,  5,2 ],
    '-x +c'    => [ 0,0,  5,2,  0,0,  5,2 ],
    '-x -c'    => [ 0,0,  5,2,  0,0,  5,2 ],

    'x y'       => [ 6,0,  6,0,  6,0,  6,0 ],
    'x a d'     => [ 5,1,  5,1,  5,1,  5,1 ],
    'x "a d"'   => [ 5,0,  5,0,  5,1,  5,1 ],

    'x AND y'       => [ 0,0,  6,0,  0,0,  6,0 ],
    'x OR y'        => [ 6,6,  6,0,  6,6,  6,0 ],
    'x AND NOT y'   => [ 5,5,  6,0,  5,5,  6,0 ],

    'x (b OR c)'        => [ 5,3,  5,2,  5,3,  5,2 ],
    'x AND (b OR c)'    => [ 3,3,  5,2,  3,3,  5,2 ],
    'x OR (b OR c)'     => [ 5,5,  5,2,  5,5,  5,2 ],
    'x (y OR c)'        => [ 6,2,  6,0,  6,2,  6,0 ],
    'x AND (y OR c)'    => [ 2,2,  6,0,  2,2,  6,0 ],
    
    'a AND NOT (b OR "c d")' => [ 1,1,  4,1,  1,3,  4,1 ],
    'a AND NOT "a b"'        => [ 1,1,  4,3,  3,0,  4,3 ],
    'a AND NOT ("a b" OR "c d")' => [ 1,1,  4,1,  0,3,  4,1 ],

    '+"b c" -d'  => [ 1,1,  2,1,  2,1,  3,1 ],
    
    'x AND NOT (b OR (c AND d))' => [ 2,2,  5,1,  2,2,  5,1 ],

);

my @syntax_tests = (
    ### spaces
    'b '        => [ 3,3 ],
    ' b'        => [ 3,3 ],
    'b c '      => [ 3,2 ],
    ' b c'      => [ 3,2 ],
    ' b c '     => [ 3,2 ],
    ' b  c '    => [ 3,2 ],
    '  b c'     => [ 3,2 ],
    
    ### malformed queries
    'x "a d'    => [ 5,0, 5,0, 5,1, 5,1 ],
    'x "a b'    => [ 5,3, 5,3, 5,3, 5,3 ],
    'x ("a d)'  => [ 5,0, 5,0, 5,1, 5,1 ],
    'x ("a b)'  => [ 5,3, 5,3, 5,3, 5,3 ],
    'x "(a d)'  => [ 5,0, 5,0, 5,1, 5,1 ],
    'x "(a b)'  => [ 5,3, 5,3, 5,3, 5,3 ],
    
    'x"'     => [ 5,5,  5,5,  5,5,  5,5 ],
    'x" '    => [ 5,5,  5,5,  5,5,  5,5 ], 
    'x"('   => [ 0,0,  0,0,  5,5,  5,5 ], # broken, but we'll just leave it.

    
    ### tokenizing
    '("-=#a")'  => [ 4,4,  4,4,  4,4,  4,4 ],
    'a "=-*b c"'  => [ 4,2, ],
);
    
for (@docs) {
    my $doc = $kindexer->new_doc($_);
    $doc->set_field( content => $_ );
    $kindexer->add_doc($doc);
}

$kindexer->generate;
$kindexer->write_kindex;
undef $kindexer;

my $kindex = Search::Kinosearch::Kindex->new( 
    -mainpath => $mainpath,
);

### Verify that KSearch is parsing boolean operators correctly, etc.
for (my $i = 0; $i <= $#logical_tests; $i += 2) {
    &test_ksearch_response($logical_tests[$i], $logical_tests[$i+1]);
}

### Verify that KSearch parses some queries with potentially problematic
### syntax correctly.
for (my $i = 0; $i <= $#syntax_tests; $i += 2) {
    &test_ksearch_response($syntax_tests[$i], $syntax_tests[$i+1]);
}

sub test_ksearch_response {
    my ($query, $responses) = @_;
    for (0 .. $#$responses) {
     #   warn "startloop";
        my $rs_garbate = Search::Kinosearch::KSearch::ResultSet->new(1,0);
        my $ksearch = Search::Kinosearch::KSearch->new(
            -kindex            => $kindex,
            -stoplist          => {},
            );
        # First 'any', then 'all'.
        my $any_or_all = $_ % 2 ? 'all' : 'any';
        my $quob = Search::Kinosearch::Query->new(
            @{ $configs[$_] },
            -string     => $query,
            -lowercase  => 1,
            -tokenize   => 1,
            -any_or_all => $any_or_all,
        );
        $ksearch->add_query($quob);
        my $status = $ksearch->process;
        my $allow_bool = $configs[$_][1];
        my $allow_phrases = $configs[$_][3];
        my $success = is($status->{num_hits}, $responses->[$_], 
        "any_or_all => $any_or_all, bool => $allow_bool, phrases => "
        . "$allow_phrases, query => $query");
        
        if (!$success) {
        #if (0) {
            $ksearch->{kindexes} = undef;
            $ksearch->{result_set}->_dump(0);
            print Dumper $ksearch;
              exit;
        }
    #    warn "stoploop";
    }
}
    
