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

my %date_docs;
my $count = 1;
sub gen_date_docs_hash {
    my @datetime;
    push @datetime, (int(rand(4000)) - 2000);
    push @datetime, (int(rand(12)) + 1);
    push @datetime, (int(rand(28)) + 1);
    push @datetime, (int(rand(24)));
    push @datetime, (int(rand(60)) + 1);
    push @datetime, (int(rand(60)) + 1);
    my $doc_content = 'a a ' x $count;
    $doc_content .= 'b';
    $count++;
    $date_docs{$doc_content} = {
        packed_date => pack ('C n C C C C C', 0, @datetime), 
        date_array  => \@datetime,
        };
}

for (0 .. 99) {
    &gen_date_docs_hash;
}

my @sorted_by_date = sort 
    { $date_docs{$b}{packed_date} cmp $date_docs{$a}{packed_date} } 
    keys %date_docs;
my @sorted_by_score = sort keys %date_docs;

#while (my ($content, $date_pieces) = each %date_docs) {
for my $content(sort keys %date_docs) {
    my $date_pieces = $date_docs{$content};
    my $doc = $kindexer->new_doc( $content );
    $doc->set_field( content => $content );
    $doc->set_datetime( @{ $date_pieces->{date_array} } );
    $kindexer->add_doc( $doc );
}
$kindexer->generate;
$kindexer->write_kindex;
undef $kindexer;

### Sanity check.
my $ksearch = Search::Kinosearch::KSearch->new( 
    -stoplist => {},
    -num_results => 100,
    );
    
my $quob = Search::Kinosearch::Query->new(
    -string => 'a', 
    -tokenize => 1, 
    );
$ksearch->add_query($quob);
$ksearch->process;
my @got;
my @scores;
while (my $result = $ksearch->fetch_hit_hashref) {
    push @got, $result->{content};    
    push @scores, $result->{score};
}
is_deeply(\@got, \@sorted_by_score, "Proper order by relevance");

### Verify sort by datetime.
@got = ();
$ksearch = Search::Kinosearch::KSearch->new( 
    -stoplist => {},
    -sort_by  => 'datetime',
    -num_results => 100,
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
is_deeply(\@got, \@sorted_by_date, "Sort by datetime works");

### Verify sort by datetime.
@got = ();
$ksearch = Search::Kinosearch::KSearch->new( 
    -stoplist => {},
    -sort_by  => 'datetime',
    -num_results => 100,
    );
$quob = Search::Kinosearch::Query->new(
    -string => '"a b"', 
    -tokenize => 1, 
    );
$ksearch->add_query( $quob );
$ksearch->process;
while (my $result = $ksearch->fetch_hit_hashref) {
    push @got, $result->{content};    
}
is_deeply(\@got, \@sorted_by_date, "Sort by datetime works with phrase queries");
