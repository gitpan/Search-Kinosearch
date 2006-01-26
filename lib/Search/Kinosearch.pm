package Search::Kinosearch;
use strict;
use warnings;

our $VERSION = '0.022';

### Coding convention:
### Public methods use hash style parameters except when to do so would cause 
### a significant performance penalty.  The parameter names are prepended with
### a dash, e.g. '-foo'.  When it is necessary to modify a parameter value
### for use, it is copied into a similarly named variable without a dash: e.g.
### $self->{-working_dir} might get copied to $self->{workdir}.

use bytes;
use attributes 'reftype';

use Carp;
use File::Spec;

use Search::Kinosearch::Lingua;

##############################################################################
### Search::Kinosearch must be subclassed.
##############################################################################
sub new {
    my $either = shift;
    my $class = ref($either) || $either;
    confess("'Search::Kinosearch' is an abstract base class. The method " .
       "new() must be redefined in the subclass '$class'");    
}

##############################################################################
### Import language-specific routines and variables from the relevant 
### Search::Kinosearch::Lingua::Xx module.
##############################################################################
sub define_language_specific_functions {
    my $self = shift;
    
    ### require the relevant Search::Kinosearch::Lingua subclass.
    no strict 'refs';
    my $language_module_name;
    if ($self->{language}) {
        $language_module_name = 'Search::Kinosearch::Lingua::' . 
            $self->{language};
    }
    else {
        $language_module_name = 'Search::Kinosearch::Lingua';
    }
    my $requirecode = "require $language_module_name";
    ### Safety check.
    die "Illegal language name" 
        unless $requirecode =~ /^require Search::Kinosearch::Lingua(::\w+)?$/;
    eval $requirecode;
    if ($@) {
       die "Errors occurred:\n$@\n... when trying to assign " .
           "language-specific functions to your object.  Are you sure " .
           "that the language you specified is supported, that its " .
           "case is correct, and that the appropriate " .
           "Search::Kinosearch::Lingua::Xx module is installed?";
    }

    ### Suck in the code.
    $self->{tokenreg} = ${$language_module_name . '::tokenreg'};
    $self->{tokenize_method} = \&{$language_module_name . '::tokenize'};
    $self->{stem_method} = \&{$language_module_name . '::stem'};
    $self->{stoplist} = 
        defined $self->{-stoplist} ? 
        $self->{-stoplist} : 
        ${$language_module_name . '::stoplist'} ?  
        ${$language_module_name . '::stoplist'} : 
        {};
    $self->{stoplist} ||= {}; # in case of '-stoplist => 0,' or similar.
}

##############################################################################
### Override the tokenizing algorithm.
##############################################################################
sub set_tokenizer {
   my $self = shift;
   my $tokenizer_coderef = shift;
   
   ### Verify that the supplied tokenizing algo meets spec.
   unless (ref($tokenizer_coderef) eq 'CODE'){
       croak "The set_tokenizer method expects to be passed a " .
           "reference to a subroutine.";
   }
   my $test = $self->$tokenizer_coderef('foo');
   unless (ref($test) eq 'ARRAY') {
       croak "Tokenizer methods must return a reference to an array";
   }
   $self->{tokenize_method}  = $tokenizer_coderef;
}

##############################################################################
### Override the stemming algorithm.
##############################################################################
sub set_stemmer {
   my $self = shift;
   my $stemmer_coderef = shift;
   
   ### Verify that the supplied stemming algo meets spec.
   unless (ref($stemmer_coderef) eq 'CODE'){
       croak "The set_stemmer method expects to be passed a " .
           "reference to a subroutine.";
   }
   my $test = $self->$stemmer_coderef('foo');
   unless (ref($test) eq 'ARRAY') {
       croak "Stemmer methods must return a reference to an array";
   }
   $self->{stem_method}  = $stemmer_coderef;
}

##############################################################################
### Set a verbosity level.
##############################################################################
sub verbosity {
    my $self = shift;
    $self->{-verbosity} = $_[0]
        if @_;
    return $self->{-verbosity};
}

1;

__END__

=head1 NAME

Search::Kinosearch - Search Engine Library

=head1 DEPRECATED

Search::Kinosearch has been superseded by L<KinoSearch|KinoSearch>.  Please
use the new version.

=head1 SYNOPSIS

First, write an application to build a 'kindex' from your document collection.
(A 'kindex' is a Kinosearch index.)

    use Search::Kinosearch::Kindexer;
    
    my $kindexer = Search::Kinosearch::Kindexer->new(
        -mainpath       => '/foo/bar/kindex',
        -temp_directory => '/foo/bar',
        );
    
    for my $field ('title', 'bodytext') {
        $kindexer->define_field(
            -name      => $field,
            -lowercase => 1,
            -tokenize  => 1,
            -stem      => 1,
            );
    }

    while (my ($title, $bodytext) = each %docs) {
        my $doc = $kindexer->new_doc( $title );
        
        $doc->set_field( title    => $title    );
        $doc->set_field( bodytext => $bodytext );
        
        $kindexer->add_doc( $doc );
    }
    
    $kindexer->generate;
    $kindexer->write_kindex;

Then, write a second application to search the kindex:

    use Search::Kinosearch::KSearch;
    use Search::Kinosearch::Query;
     
    my $ksearch = Search::Kinosearch::KSearch->new(
        -mainpath   => '/foo/bar/kindex',
        );
    
    my $query = Search::Kinosearch::Query->new(
        -string     => 'this AND NOT (that OR "the other thing")',
        -lowercase  => 1,
        -tokenize   => 1,
        -stem       => 1,
        );
    $ksearch->add_query( $query );
    $ksearch->process;
    
    while (my $hit = $ksearch->fetch_hit_hashref) {
        print "$hit->{title}\n";
    }

=head1 DESCRIPTION

=head2 Primary Features

=over

=item

Match 'any' or 'all' search terms

=item

Match phrases 

=item

Boolean operators AND, OR, and AND NOT

=item

Support for parenthetical groupings of arbitrary depth

=item

Prepended +plus or -minus to require or negate a term

=item

Sort results by relevance or by datetime

=item

Stemming

=item

Algorithmic selection of relevant excerpts

=item

Hilighting of search terms in excerpts, even when stemmed

=item

Fast, efficient algorithms for both indexing and searching

=item

Works well with large or small document collections

=begin comment

### DEVNOTE: extension to multiple languages is planned after the core is
### stabilized.

=item

L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua> modules provide
Kinosearch functionality in multiple languages.

=end comment

=item

High quality ranking algorithm based on term frequency / inverse document
frequency (tf/idf)

=back


=head2 General Introduction

Search::Kinosearch (hereafter abreviated 'Kinosearch') is a search engine
library -- it handles the tricky, behind-the-scenes work involved in running a
search application.  It is up to you how to present the search results to the
end-user.

Kinosearch has two main parts:
L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer> and
L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch> (hereafter
abbreviated 'Kindexer' and 'KSearch').  When you want to know which pages of a
book are most relevant for a given subject (e.g. avocados, Aesop, Alekhine's
Defense...) you look up the term in the book's index and it tells you which
pages may be of interest to you; the 'kindex' produced by Kinosearch's
Kindexer performs an analogous role -- using the interface tools provided by
the KSearch module, you consult the kindex to find which documents within the
document collection Kinosearch considers most relevant to your query. 

[The Search::Kinosearch module itself doesn't do very much, and as an abstract
base class, it does nothing on its own; this documentation is an overview
which ties together the various components of the Kinosearch suite.]  

The Kindexer thinks of your documents as database rows, each with as many
fields as you define.  HTML documents might be parsed, prepared, and fed to
the Kindexer like so:

=over

=item 1

Store the URL in a kindex field called 'url'.

=item 2

Store the text surrounded by the E<lt>titleE<gt> tags in a kindex field called
'title';

=item 3

Isolate portions of the document that are not relevant to content (such as
navigation panels or advertisements) and remove them.

=item 4

Strip all html tags.

=item 5

Store what's left in a field called 'bodytext'.

=back

Most of the time, you will want to take advantage of three crucial functions
performed by the Kindexer, executed on a per-field basis depending on the
parameters passed to define_field(): 

=over

=item -lowercase

This does exactly what you would expect - lc the text to be indexed (but
leave the copy of the text to be stored intact).  If you select the same
option for your queries at search-time, your searches will be
case-insensitive.

=item -tokenize

Tokenizing breaks up input text into an array of words (roughly speaking).  If
you tokenize the text "Dr. Strangelove, or How I Learned to Stop Worrying and
Love the Bomb", then searches for "Strangelove" or "Bomb" will match.  If you
don't tokenize it, then only a search for the complete string "Dr.
Strangelove, or How I Learned to Stop Worrying and Love the Bomb" will match.

=item -stem

Stemming reduces words to a root form.  For instance, "horse", "horses",
and "horsing" all become "hors" -- so that a search for 'horse' will also
match documents containing 'horses' and 'horsing'.  For more information, see
the documentation for L<Lingua::Stem|Lingua::Stem>.

=back

Once you have the completed the indexing phase, you will need a search
application.  Most often, this takes the form of a CGI script accessed via web
browser.  The interface design requirements of such an app will be familiar to
anyone who's surfed the web.  

=head2 Getting started

If you want to get started right away, copy the sample applications out of 
L<Search::Kinosearch::Tutorial|Search::Kinosearch::Tutorial>, get them
functioning, and then swap in some of your own material.

You may wish to consult the documentation for
L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer>,
L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch> and
L<Search::Kinosearch::Query|Search::Kinosearch::Query> before continuing on to
the next section.

=head1 Fine Tuning Kinosearch

=head2 Performance Optimizations

The bottleneck in search applications is the ranking and sorting of a large
number of relevant documents.  Minimizing the time it takes to return results
is a central design goal of Kinosearch.

The single most important optimization available for Kinosearch apps is to
store either some or all of the kindex files in ram -- in particular, the
files stored within the directory specified by the '-freqpath' parameter.
These files contain all of the kindex data upon which search-time speed
depends.  Storing the other kindex files in ram won't hurt, but will not yield
anywhere near the same benefit.

An additional search-time optimization is available when running Kinosearch
applications under mod_perl.  See the
L<Search::Kinosearch::Kindex|Search::Kinosearch::Kindex> documentation for
details.

=head2 Stoplists

A "stoplist" is collection of "stopwords": words which are common enough to be
of little use when determining search results.  For example, so many documents
in English contain "the", "if", and "maybe" that it is best if they are
blocked, not just at search time, but at index time.  Removing them from the
equation saves time and space, plus improves relevance.  

By default, the Kindexer excludes a small list of stopwords from the kindex
and KSearch reports when it detects any of them in a search query.  It is
possible to disable the stoplist or use a different one, if you so desire.

=head2 Phrase Matching Algorithm

Kinosearch's phrase matching implementation involves storing concatenated
pairs of words in the kindex.  This strategy yields a substantial performance
benefit at search time (since the extremely fast hash-based algorithm used to
scan for individual terms can be extended to detect phrases as well), but at
the cost of increased kindex size.  Blocks of text are broken into
overlapping couplets, which are stored as individual terms in the kindex --
e.g. the text "brush your teeth" produces four kindex entries: "brush",
"teeth", "brush your", and "your teeth" ("your" on its own is a stopword, so
it is excluded from the kindex by default).  If a user searches for the the
specific phrase "brush your teeth", Kinosearch will return only documents
which contain I<both> "brush your" and "your teeth".  

=head2 Ranking Algorithm

Kinosearch uses a variant of the well-established "Term Frequency / Inverse
Document Frequency" weighting scheme.  Explaining TF/IDF is beyond the scope
of this documentation, but in a nutshell: 

=over

=item

in a search for "skate park", documents which score well for the comparatively
rare term "skate" will rank higher than documents which score well for the
common term "park".  

=item

a 10-word text which has one occurrence each of both "skate" and "park" will
rank higher than a 1000-word text which also contains one occurrence of each.

=back

A web search for "tf idf" will turn up many excellent explanations of the
algorithm.


=begin comment

# DEVNOTE: descending term weighting is not implemented.  Default phrase
# matching has been disabled.  

# Additionally, Kinosearch has phrase matching
# and descending term weighting silently enabled by default, working on the
# assumption that the order of words in user-supplied search phrases is
# probably meaningful.  In a search for C<bone soup>, documents which match
# the phrase "bone soup" will score more highly than documents which contain
# "bone" and "soup" in isolation, and documents which are highly relevant for
# the first term "bone" will receive a small boost over documents which are
# highly relevant for the second term "soup".  Searching for C<soup bone> will
# likely produce a different result.  This behavior mimics that of many
# well-established web search engines.

=end comment

=head2 Field Weights

Kinosearch will allow you to weight the title of a document more heavily than
the bodytext -- or vice versa -- by assigning a weight to each field at
either index-time or search-time.  However, the multipliers don't have to be
that large, because under TF/IDF a single occurrence of a word within the
10-word title will automatically contribute more to the score of a document
than a single occurrence of the same word in the 1000-word bodytext.  See the
documentation for Kindexer's define_field() method for more details. 

=begin comment

# =head2 Doc Rank DEVNOTE: not yet implemented
# 
# Kinosearch assumes that it should score documents based solely on their
# content unless you tell it otherwise.  If you want some documents to rank
# higher or lower in the results than their content would indicate, you can
# assign them a "docrank" other than C<1> at index time.  

=end comment

=begin comment

### DEVNOTE: new() and define_language_specific_functions() are private.  The
### others are experimental.

=head1 CONSTRUCTOR

=head2 new()

=head1 METHODS

=head2 define_language_specific_functions()

=head2 set_stemmer()

=head2 set_tokenizer()

=head2 verbosity()

=end comment

=head1 TO DO

=over

=item

Overhaul the API.  Kinosearch's major classes have grown too large, and need to
be reimplemented as extensible abstract base classes.  

For instance, the current version of Search::Kinosearch::Kindex is limited
because it specifies particular file configurations and tie classes.  It is
being reworked as a a Perl complex data structure which represents only the
abstract format of a kindex.  Happily, the new version will work fine as an
in-memory kindex on its own, speeding testing and development.  Subclasses,
such as the Search::Kinosearch::Kindex::FS, will be implemented primarily
through the use of Perl's TIE mechanism.

=item

Dispense with DB_File altogether and implement a composite index format.

=item

Shrink proximity data.  Right now it's solid packed 32 bit integers.

=item

Enable customizable tokenizing and stemming.

=item

Implement docrank.

=item

Fix excerpt highlighting for phrases so that a search for 'we the people'
doesn't cause every instance of 'the' to be highlighted. -- DONE in
development version.

=item

Complete support for UTF-8.  At present, everything works so long as the
encoding of all supplied files is ASCII compatible and is consistent across
all files. 

=item 

Add unicode normalization.

=item

Add support for other encodings.

=item

Implement -cache_level option for Search::Kinosearch::Kindexer, in order to
allow user control over memory requirements.  This feature is dependent on the
implementation of -mem_threshold in L<Sort::External|Sort::External>,
being tested as of this writing.

=item 

Implement descending term weighting.

=item

Add support for 64-bit timestamps.

=item

Enable support for psuedo-stemming using
L<Lingua::Spelling::Alternative|Lingua::Spelling::Alternative> for languages
where no Snowball stemmer is available.

=back

=head1 BUGS

=over

=item

Excerpt may be up to 20 characters longer than requested. 

=item

Spurious results can turn up in searches for phrases 3
terms or longer: for instance, a document containing "I<brush your> hair and
floss I<your teeth>" will be returned in a search for '"brush your teeth"'. --
FIXED in development version.

=back

=head1 FILES

Kinosearch requires several other CPAN distributions:

=over

=item

L<Sort::External|Sort::External>

=item

L<Test::Exception|Test::Exception>

=item

L<Lingua::Stem::Snowball|Lingua::Stem::Snowball>

=item

L<Compress::Zlib|Compress::Zlib>

=item

L<DB_File|DB_File>

=item

L<String::CRC32|String::CRC32>

=back

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer>

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=item

L<Search::Kinosearch::Query|Search::Kinosearch::Query>

=item

L<Search::Kinosearch::Kindex|Search::Kinosearch::Kindex>

=item

L<Search::Kinosearch::Tutorial|Search::Kinosearch::Tutorial>

=back

=head1 ACKNOWLEDGEMENTS

Chris Nandor has been helpful with debugging.

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

