package Search::Kinosearch::KSearch;
use strict;
use warnings;

use base qw( Search::Kinosearch Search::Kinosearch::QueryParser );
use Search::Kinosearch::Kindex;
use Search::Kinosearch::Query;
use Search::Kinosearch::QueryParser;
use Search::Kinosearch::Doc;

use attributes 'reftype';

use Carp;
use Storable qw( nfreeze thaw );
use String::CRC32 'crc32';

##############################################################################
### Create a new Search::Kinosearch::Ksearch object.
##############################################################################
sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->_init_ksearch(@_);
    return $self;
}

my %init_ksearch_defaults = (
    -mainpath          => undef,
    -freqpath          => undef,
    -kindex            => undef,
    
    -sort_by           => 'score',
    -ascdesc           => 'descending',
    -max_terms         => undef,
    -num_results       => 10,
    -offset            => 0,
    
    -language          => 'En',
    -stoplist          => undef,
       
    -excerpt_field     => undef,
    -excerpt_length    => 150,
    -hl_tag_open       => '<strong>',
    -hl_tag_close      => '</strong>',
    ### The production type.  'taproot', because it's lower than trunk, and
    ### the word 'root' might be confusing.
    prodtype           => 'taproot',
);

##############################################################################
### Initialize a KSearch object.
##############################################################################
sub _init_ksearch {
    my $self = shift;
    %$self = (%init_ksearch_defaults, %$self);

    while (@_) {
        my $var = shift;
        my $val = shift;
        croak("Invalid parameter: $var")
            unless exists $init_ksearch_defaults{$var};
        $self->{$var} = $val;
    }
    
    if (defined $self->{-kindex}) {
        croak ("-kindex parameter isn't a Search::Kinosearch::Kindex")
            unless $self->{-kindex}->isa("Search::Kinosearch::Kindex");
        $self->{kindex} = $self->{-kindex};
    }
    else {
        $self->{kindex} = Search::Kinosearch::Kindex->new(
            -mainpath   => $self->{-mainpath},
            -freqpath   => $self->{-freqpath},
            );
    }
    $self->{language} = $self->{-language};
     
    ### Where queries accumulate.
    $self->{productions} = [];
    ### Used for excerpt highlighting.
    $self->{searchterms} = [];
    
    ### TODO separate QueryParser from KSearch completely.
    $self->_init_queryparser;
    
    ### Define tokenizing, stemming, $tokenreg, stoplist, etc.
    $self->define_language_specific_functions;
}

##############################################################################
### Add a query to a KSearch object.
##############################################################################
sub add_query {
    my $self = shift;
    my $query = shift;
    croak("Expecting a Search::Kinosearch::Query object") 
        unless (ref($query) and $query->isa('Search::Kinosearch::Query'));
    ### Each "production" at the root level of the KSearch object is a query.
    ### In order to make the recursive parser work correctly, all the levels 
    ### must be named the same -- hence "productions".  
    push @{ $self->{productions} }, $query;
}

##############################################################################
### Generate a result set by processing all queries. 
##############################################################################
sub process {
    my $self = shift;

    return unless $self->{productions};

    $self->parse;

    $self->_retrieve_term_scores($_, $_) foreach @{ $self->{productions} };
    $self->_recursive_score($self);

    if ($self->{-excerpt_field}) {
        $self->_create_searchterm_list($self);
    }
    my @searchterm_crcs 
        = map { crc32($_) } @{ $self->{searchterms} };

    my @in;
    ### '' in the result set is a useful kludge.  See below. 
    delete $self->{result_set}{''} if exists $self->{result_set}{''};
    ### Perform a 'packed default sort.
    if ($self->{-sort_by} eq 'score') {
        while (my ($doc_tag, $score) = each %{ $self->{result_set} } ) {
            push @in, (pack("N", $score) . $doc_tag);
        }
    }
    else {
        while (my ($doc_tag, $string) = each %{ $self->{result_set} } ) {
            push @in, ($string . $doc_tag);
        }
    }
    my @out = $self->{-ascdesc} eq 'descending' ?
              (sort { $b cmp $a } @in) :
              (sort @in);
              
    $self->{status_hash}{num_hits} = @out;

    my @ranked_results;
    my $first = $self->{-offset};
    my $last = $first + $self->{-num_results} - 1;
    my $valid_hits = 0;
    
    my $pack_template;
    if ($self->{-sort_by} eq 'score') {
        $pack_template = 'N C N ';
    }
    else {
        my $bytes_per_hit = length($out[0]);
        $bytes_per_hit -= 5;
        $pack_template = "A$bytes_per_hit C N";
    }
    
    ### Assemble a hit list.
    my $storable_fields = $self->{kindex}{fields_store};
    my $num_storable_fields = @$storable_fields;
    while (my $hit = shift @out) {
        last if $valid_hits > $last;
        my ($score_or_string, $knum, $doc_num)
                = unpack ($pack_template, $hit);
        next if exists 
            $self->{kindex}{kinodel}{$doc_num}{$knum};
        ### If it isn't a deletion, it's a valid hit.
        $valid_hits++;
        ### If we haven't reached the offset yet, go to the next hit.
        next unless $valid_hits > $first;
        ### Retrieve stored document fields, and put them in a Doc object.
        my $docdata = $self->{kindex}{subkindexes}[$knum]{kinodocs}{$doc_num};
        my %stored_fields;
        
        my @field_lengths 
            = unpack('N*', substr($docdata, 0, ($num_storable_fields*4)));
        my $len_stored = $num_storable_fields*4;
        $len_stored += $_ for @field_lengths;

        my $stored_fields_tpt = 'a' . ($num_storable_fields*4) . ' ';
        $stored_fields_tpt .= "a$_ " for @field_lengths;
        (undef, @stored_fields{@$storable_fields}) = 
            unpack($stored_fields_tpt, substr($docdata, 0, $len_stored, ''));

        my $num_tokens = unpack('N', substr($docdata, 0, 4));
        my @token_crcs;
        (undef, @token_crcs) = unpack('N*',
             substr($docdata, 0, ($num_tokens*4 + 4),''));
        my @token_freqs = unpack('N*',
             substr($docdata, 0, ($num_tokens*4),''));
        my $token_infos_tpt = '';
        $token_infos_tpt .= "a$_ " for @token_freqs;
        my %token_infos;
        @token_infos{@token_crcs} = unpack($token_infos_tpt, $docdata);

        my @positions;
        for (@searchterm_crcs) {
            next unless exists $token_infos{$_};
            push @positions, unpack('N*', $token_infos{$_});
        }
        my ($excerpt_field_start, $excerpt_field_length) = (0,0);
        for (0 .. $#$storable_fields) {
            if ($self->{-excerpt_field} = $storable_fields->[$_]) {
                $excerpt_field_length = $field_lengths[$_];
                last;
            }
            else {
                $excerpt_field_start += $field_lengths[$_];
            }
        }
        $_ -= $excerpt_field_start for @positions;    
        @positions 
            = grep { $_ >= 0 and $_ < $excerpt_field_length } @positions;

        my $doc = Search::Kinosearch::Doc->new( 
            { fields => \%stored_fields } );
        push @ranked_results, $doc;
        if ($self->{-sort_by} eq 'score') {    
            $doc->{fields}{score} = $score_or_string/32_000;
        }
        if ($self->{-excerpt_field}) {
            $doc->set_field( 
                excerpt => $self->_create_excerpt($knum, $doc, \@positions) );
        }
    }
    $self->{ranked_results} = \@ranked_results;
    $self->{status_hash}{num_docs} = $self->{num_docs};
    return $self->{status_hash}; 
}

### Like many recursive routines, the flow of the _recursive_score 
### subroutine is difficult to follow.  This verbose explanation is 
### intended to compensate for the fact that even heavily commented, the code
### doesn't do a good job of documenting itself.
### 
### The seed for the _recursive_score subroutine is itself the product of
### another recursive routine, Search::Kinosearch::QueryParser->parse.
### 
### Each level in the hierarchy of the parsed Query object consists of 
### productions, and information about those productions.  At the lowest level, an 
### atomic production consists of a single search term.  _recursive_score
### walks this hierarchy.
### 
### Each production produces a result set that may or may not be either required 
### or negated.  First, the result set of individual search terms is 
### calculated.  Then result sets are merged using set math.  The order 
### in which the results are calculated and added is optimized for the 
### minimum processing possible, mostly by keeping the result set as small 
### as possible at all times.
### 
### The search phrase 'this OR (that AND NOT "the other thing")' is broken 
### into productions as follows:
### 
### PRODUCTION_1 consists of the entire search string:
### 
### PRODUCTION_1 => 'this OR (that AND NOT "the other thing")'
### 
### The result set for PRODUCTION_1 is the merger of two other productions:
### 
### PRODUCTION_2 => 'this'
### PRODUCTION_3 => 'that AND NOT "the other thing"'
###
### Scores for each document in the result set are accumulative, so a 
### document with a score of 2.1 for PRODUCTION_2 and 3.1 for PRODUCTION_3 will yield 
### a score of 5.2 when the sets are merged. (More or less.)
### 
### The result set for PRODUCTION_2 => 'this' is all documents which match the term 
### 'this' and their associated scores.
### 
### The result set for PRODUCTION_3 'that AND NOT "the other thing"' is derived by 
### merging the result sets for two other productions.
### 
### PRODUCTION_4 is all documents which match 'that'.
###
### PRODUCTION_5 is itself the merger of two other result sets, PRODUCTION_6 and PRODUCTION_7.
### 
### PRODUCTION_6 is all documents which match the search term 'the other'
### PRODUCTION_7 is all documents which match the search term 'other thing'
###
### Because both 'the other' and 'other thing' must be present, the result
### set for PRODUCTION_5 consists of the *intersection* of PRODUCTION_6 and PRODUCTION_7.  
### Kinosearch knows to do this because *both* are marked as "required".
### 
### PRODUCTION_3 'that AND NOT "the other thing"' is derived by subtracting the 
### result set for PRODUCTION_5 '"the other thing"' from the result set for 
### PRODUCTION_4 'that'. Kinosearch knows to subtract PRODUCTION_5 because it is 
### marked with the "negated" flag;.
###
### Using the "required" and "negated" flags and recursive algorithms, 
### it is possible to analyze boolean phrase queries of arbitrary depth.

sub _recursive_score {
    my $self = shift;
    my $sprout = shift;

    return unless $sprout->{productions};
    return if $sprout->{prodtype} eq 'terminal';
    
    ### Sort productions into the order which yields the quickest set mergers.
    my @sorted_productions = sort {
        $b->{negated}  <=> $a->{negated}
                       or
        $b->{required} <=> $a->{required}
                       or
        $a->{set_size} <=> $b->{set_size}
        } (@{ $sprout->{productions} });

    $self->_recursive_score($_) foreach @sorted_productions;
    
    my $reqlist = {};
    my $neglist = {};
    my $result_set = {};
    
    ### If the parent node has only one child, then inheritance is
    ### straightforward -- the result set passes from parent to child
    ### by reference (without any modification).
    if ((@{ $sprout->{productions} } == 1) 
        and ($sprout->{productions}[0]{set_size})) 
    {
        if ($sprout->{productions}[0]{negated}) {
            $sprout->{result_set} = {};
            $sprout->{set_size} = 0;
        }
        else {
            $sprout->{result_set} = $sprout->{productions}[0]{result_set};
            $sprout->{set_size} = $sprout->{productions}[0]{set_size};
        }
        return;
    }
    
    ### Combine the reqlists and neglists for all the productions.  
    ### We do this first so we can filter before we score.
    foreach my $production (@sorted_productions) {
        next if $production->{prodtype} eq 'boolop';
        if ($production->{negated}) {
            if (%$neglist) {
                my $more_negations = $production->{result_set};
                @{ $neglist->{ keys %$more_negations } } 
                    = values %$more_negations;
            }
            else {
                $neglist = $production->{result_set};
            }
            next;
        }
        unless (%$neglist or %$reqlist or %$result_set) {
            $result_set = $production->{result_set};
            $result_set ||= {}; # This shouldn't have been necessary but it
                                # fixed a crashing bug.
            if ($production->{required}) {
                %$reqlist = %$result_set;
            }
            next;
        }
        
        ### Build a routine for deriving a result set from an undetermined
        ### number of productions, each of which may be required, negated, or
        ### neither, and which must be sorted either by accumulated score or 
        ### by date.
        ### TODO Is it possible to make this less tortured? 
        my $merged = { '' => undef };
        my $routine_part1 = q(
            while (my ($doc_tag, $score_or_string) 
                = each %{ $production->{result_set} })
            {
            );
        my $routine_part2 = $self->{-sort_by} eq 'score'     ?
            q(
                $result_set->{$doc_tag} += $score_or_string;
            })                                                   :
            q(
                $result_set->{$doc_tag} = $score_or_string;
            });

        if (%$neglist) {
            $routine_part1 .= q(
                next if exists $neglist->{$doc_tag};);
        }
        
        if (%$reqlist) {
            $routine_part1 .= q(
                next unless exists $reqlist->{$doc_tag};);
        }
        
        ### If there's already a reqlist, AND this production is required,
        ### then generate a new reqlist consisting of the intersection of the
        ### two sets.
        if (%$reqlist and $production->{required}) {
            $routine_part1 .= q(
                $merged->{$doc_tag} = undef;); 
            $routine_part2 .= q(
                foreach (keys %$result_set) {
                    delete $result_set->{$_} unless exists $merged->{$_};
                }
                $reqlist = $merged;);
        }
        
        elsif ($production->{required}) {
            $routine_part2 .= q(
                $reqlist = $production->{result_set};            
                ### Make %$reqlist evaluate to true, even if it contains no
                ### docs -- because if you require a term, and it doesn't exist
                ### in any documents, your query shouldn't return anything.
                $reqlist->{''} = undef;);
        }

        eval $routine_part1 . $routine_part2;
        die $@ if $@;
    }
    $sprout->{result_set} = $result_set;
    $sprout->{set_size} = scalar keys %$result_set;
}

sub _retrieve_term_scores {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    if (defined $sprout->{productions}) {
        $self->_retrieve_term_scores($_, $trunk) foreach @{ $sprout->{productions} }
    }
    
    ### We can only retrieve term scores for terms stored in the kindex -- ie,
    ### not phrases, boolean operators, etc.
    return unless $sprout->{prodtype} eq 'terminal';
    
    my $kindex = $self->{kindex};
    my $term = $sprout->{qstring};
    
    $sprout->{set_size} = 0;
    
    for my $knum (0 .. $#{ $kindex->{subkindexes} }) {
        my $subkindex = $kindex->{subkindexes}[$knum];
        if (length $term and exists $subkindex->{kinoterms}{$term}) {
            ### The first number in the kinoterms entry has multiple names 
            ### and multiple purposes.  Technically, it tells you how many 
            ### entries to read in the kinodata binary files.  It also 
            ### tells you how many documents the term appeared in out of 
            ### the collection, which is used in the idf weighting 
            ### formula.  Third, it indicates the size of the result set 
            ### for this production -- Kinosearch uses set size to 
            ### determine the most efficient order in which to process 
            ### productions.
            my %entry;
            @entry{'doc_freq','offset','filenum'} 
                = unpack('N N N', $subkindex->{kinoterms}{$term});
            $sprout->{kinoterms_entries}[$knum] = \%entry;
            $sprout->{set_size} += $entry{doc_freq};
        }
    }
    
    if ($sprout->{set_size} == 0) {
        ### If we didn't do this, then 'stuff +does_not_exist_in_kindex' would
        ### return the result set for stuff, rather than an empty list.
        $sprout->{result_set} = { '' => undef } if $sprout->{required};
        return;
    }

    my %result_set;

    my $total_doc_freq = $sprout->{set_size};
    
    my $num_docs = $kindex->{num_docs};
    my $entries_per_kdata_file = $kindex->{entries_per_kdata_file};
    my %scorefields;
    if (ref $sprout->{-fields}) {
        %scorefields = %{ $sprout->{-fields} };
    }
    ### Use the aggregate score by default.
    $scorefields{aggscore} = 1 unless scalar keys %scorefields;
    my @scorefields_arr = sort keys %scorefields;
    for (@scorefields_arr) {
        croak("Illegal field: '$_'") 
            unless exists $kindex->{kdata_bytes}{$_};
    }

    ### The +1 makes it possible to discriminate when all docs contain a term.
    my $idf  = log(($num_docs+1)/$total_doc_freq);
    
    my $bytes = $kindex->{kdata_bytes};
    ### Prepare the multipliers for each field we're going to score on.
    ### We'd have to multiply each hit's score by idf anyway.  It's more
    ### efficient to prepare a single multiplier consisting of weight * idf
    ### than it is to multiply each result by weight and idf separately.
    my %factors;
    while (my ($field, $weight) = each %scorefields) {
        $factors{$field} = $idf * $weight;
        $bytes->{$field} = $kindex->{kdata_bytes}{$field};
        croak("Can't score field '$field'") 
            if ($bytes->{$field} != 2 and $self->{-sort_by} eq 'score');
    }
        
    my $min_sortstring = $trunk->{-min_sortstring};
    my $max_sortstring = $trunk->{-max_sortstring};
    my ($min_datestring, $max_datestring);
    if (defined $trunk->{-min_date}) {
        my @zeropad = (0) x (6 - @{ $trunk->{-min_date} });
        ### The extra c at the top will go away when we move to 64bit 
        ### timestamps.
        $min_datestring = pack('c n c c c c c', 
            0, @{ $trunk->{-min_date} }, @zeropad)
    }
    if (defined $trunk->{-max_date}) {
        my @zeropad = (0) x (6 - @{ $trunk->{-max_date} });
        $max_datestring = pack('c n c c c c c', 
            0, @{ $trunk->{-max_date} }, @zeropad)
    }
    
    SUBKINDEX: for my $knum (0 .. $#{ $kindex->{subkindexes} }) {
        next unless (exists $sprout->{kinoterms_entries}[$knum]
            and defined $sprout->{kinoterms_entries}[$knum]);
        my $subkindex = $kindex->{subkindexes}[$knum];
        
        my $doc_freq = $sprout->{kinoterms_entries}[$knum]{doc_freq};

        my $scorefile_num =
            $sprout->{kinoterms_entries}[$knum]{filenum};
        ### offset in this context refers to the start point for 
        ### relevant data in the binary .kdt file.
        my $offset = $sprout->{kinoterms_entries}[$knum]{offset};
        
        my $lines_to_grab = $doc_freq;
        my $starts_and_lengths = [ $scorefile_num, $offset, $doc_freq ];

        ### limit the addresses of data to be read to data within the
        ### sortstring range.
        if (defined $min_sortstring or defined $max_sortstring) {
            $starts_and_lengths = &_analyze_range_query( 
                $self, $subkindex, 'sortstring', $min_sortstring, $max_sortstring, 
                $starts_and_lengths);
        }
        ### limit the addresses of data to be read to data within the
        ### datetime range.
        if (defined $min_datestring or defined $max_datestring) {
            $starts_and_lengths = &_analyze_range_query( 
                $self, $subkindex, 'datetime', $min_datestring, $max_datestring, 
                $starts_and_lengths);
        }
        
        my %packed_data;
        $packed_data{$_} = '' for @scorefields_arr;
        my $packed_doc_nums = ''; 
        my $packed_datetimes = '';
        ### Retrieve the relevant section of kinodata, from multiple files 
        ### if necessary. 
        while (@$starts_and_lengths) {
            my @args = splice(@$starts_and_lengths,0,3);
            for my $scorefield (@scorefields_arr) {
                $packed_data{$scorefield} .= $kindex->_read_kinodata(
                    $subkindex, $scorefield, @args);
            }
            $packed_doc_nums .= $kindex->_read_kinodata(
                $subkindex, 'doc_num', @args);
            if ($self->{-sort_by} eq 'datetime') {
                $packed_datetimes .= $kindex->_read_kinodata(
                    $subkindex, 'datetime', @args);
            }
        }
        next SUBKINDEX unless length($packed_doc_nums);
        
        ### The temp templates are needed for perl 5.6 compatibility.
        ### (instead of using unpack("($kinodata_template)*" ...
        my $reps = length($packed_doc_nums)/4;
        my $temp_dnum_tpt = "a4 " x $reps;
        my %temp_templates;
        if ($self->{-sort_by} eq 'score') {
            $temp_templates{$_} = 'n ' x $reps 
                for @scorefields_arr;
        }
        else {
            $temp_templates{$_} = "a$bytes->{$_} " x $reps 
                for (@scorefields_arr, 'datetime');
        }
        my $packed_kindexnum = pack ('C', $knum);

        my $first_sf = $scorefields_arr[0];
        my $scores
            = [ unpack($temp_templates{$first_sf}, $packed_data{$first_sf}) ];
        ### If the search specifies more than one field, we have to add up the
        ### scores for each field.
        if (@scorefields_arr > 0) {
            my $new_scores = [];
            for my $scorefield (@scorefields_arr[1 .. $#scorefields_arr]) {
                my $factor = $factors{$scorefield};
                ### This if/else is only for efficiency's sake.
                ### The only difference is multiplying by $factor.
                if ($self->{-sort_by} eq 'score' and $factor != 1) {
                    @$new_scores = 
                        map { $_ * $factor + shift @$scores } 
                            unpack($temp_templates{$scorefield}, 
                                   $packed_data{$scorefield});
                    $scores = $new_scores;
                    $new_scores = []; 
                }
                else {
                    @$new_scores = 
                        map { $_ + shift @$scores } 
                            unpack($temp_templates{$scorefield}, 
                                   $packed_data{$scorefield});
                    $scores = $new_scores;
                    $new_scores = []; 
                }
            }
        }
        if ($self->{-sort_by} eq 'score') {
            @result_set{ 
                    map { "$packed_kindexnum$_" } 
                    unpack($temp_dnum_tpt, $packed_doc_nums) 
                } = @$scores;
        }
        ### If it's a sort by datetime AND -fields is specified...
        elsif (ref $sprout->{-fields} and %{ $sprout->{-fields} }) {
            my @doc_nums = unpack($temp_dnum_tpt, $packed_doc_nums);
            my @datetimes 
                = unpack($temp_templates{datetime}, $packed_datetimes);
            for (@$scores) {
                if ($_ == 0) {
                    shift @datetimes;
                    shift @doc_nums;
                    next;
                }
                $result_set{ $packed_kindexnum . shift @doc_nums } 
                    = shift @datetimes;
            }
        }
        ### If it's sort by datetime, but we're using aggscore so all docs are
        ### definitely relevant...
        else {
            @result_set{ 
                    map { "$packed_kindexnum$_" } 
                    unpack($temp_dnum_tpt, $packed_doc_nums) 
                } = unpack($temp_templates{datetime}, $packed_datetimes);
        }
            
    }
    ### We only have to filter if -fields has been specified.
    if ($self->{-sort_by} eq 'score' 
        and ref $sprout->{-fields} 
        and %{ $sprout->{-fields} }) 
    {
        while (my ($doc_tag, $score) = each %result_set) { 
            delete $result_set{$doc_tag} if $score == 0;
        }
    }
    $sprout->{result_set} = \%result_set;
}

### Limit the amount of data to be read by the to sections of kinodata which
### match a string range criteria.  
sub _analyze_range_query {
    my ($self, $subkindex, $field, $min, $max, $starts_and_lengths) = @_;
    my $kindex = $self->{kindex};
    my $entries_per_kdata_file = $kindex->{entries_per_kdata_file};
    my $bytes = $kindex->{kdata_bytes}{$field};

    $min = defined $min ?  $min : ("\0" x $bytes);
    $max = defined $max ?  $max : ("\377" x $bytes);

    use bytes;
    my $len = length($min);
    if ($len == $bytes) { ; }
    elsif ($len < $bytes) {
        $min .= "\0" x ($bytes - $len);
    }
    elsif ($len > $bytes) {
        $min = substr($min, 0, $bytes);
    }
    $len = length($max);
    if ($len == $bytes) { ; }
    elsif ($len < $bytes) {
        $max .= "\377" x ($bytes - $len);
    }
    elsif ($len > $bytes) {
        $max = substr($max, 0, $bytes);
    }

    my @s_a_l;
    while (@$starts_and_lengths) {
        my ($fnum, $offs, $len) = splice(@$starts_and_lengths,0,3);
        my $packed_data = $kindex->_read_kinodata(
            $subkindex, $field, $fnum, $offs, $len);
        my $was_in_range = 0;
        my $st;
        my $fn = $fnum;
        ### TODO test for perl version when creating template.
        my $template = "a$bytes " x $len;
        for (unpack($template, $packed_data)) {
            ++$offs;
            if ($was_in_range) {
                next unless ($_ lt $min or $_ gt $max);
                my $l = ($offs-1) - $st;
                push @s_a_l, ($fn, $st, $l);
                $was_in_range = 0; 
            }
            else {
                next unless ($_ ge $min and $_ le $max);
                $st = ($offs - 1) % $entries_per_kdata_file;
                $fn = $fnum + int($offs/$entries_per_kdata_file);
                $was_in_range = 1;
            }
        } 
        if ($was_in_range) {
            my $l = $offs - $st;
            push @s_a_l, ($fn, $st, $l);
        }
    }
    return \@s_a_l;
}

sub fetch_hit_hashref {
    my $self = shift;
    if (my $ranked_result =  shift @{ $self->{ranked_results} }) {
        return $ranked_result->{fields};
    }
}

##############################################################################
### Create a list of terms to be used for highlighting later.
##############################################################################
sub _create_searchterm_list {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    if ($sprout->{prodtype} eq 'terminal') {
        push @{ $self->{searchterms} }, $sprout->{qstring}
            if (!$trunk->{fields} 
                or exists $trunk->{fields}{"$self->{excerpt_field}"});
    }
    return unless defined $sprout->{productions};
    $self->_create_searchterm_list($_, $trunk) for @{ $sprout->{productions} };
}


##############################################################################
### Create a relevant excerpt from a document, with search terms highlighted
### if desired. 
##############################################################################
sub _create_excerpt {
    
    my $self = shift;
    my $knum = shift;
    my $doc = shift;
    my $posits = shift;
    
    my $fieldname = $self->{-excerpt_field};
    my $doc_num = $doc->get_field('doc_num');
    my $text = $doc->get_field( $fieldname );

    return '' unless length $text;

    my $subkindex = $self->{kindex}{subkindexes}[$knum];
    my $excerpt_length = $self->{-excerpt_length};
    my $hl_tag_open = $self->{-hl_tag_open};
    my $hl_tag_close = $self->{-hl_tag_close};
    my $tokenreg = $self->{tokenreg};

    my $best_loc;
    my %locations;
    @locations{@$posits} = (0) x @$posits if @$posits;
    my $fieldnumber = 0;
    for (@{ $self->{kindex}{fields_sort} }) {
        last if $_ eq '$fieldname';
        $fieldnumber++;
    }
    ### Create a hash where the keys are all position markers indicating the
    ### start of a keyword.
#    foreach my $term (@{ $self->{searchterms} }) {
#        my $packed_position_data = $kinoprox_entry->{$fieldnumber}{$term};
#        if (defined $packed_position_data) {
#            my @positions = unpack("N*", $packed_position_data);
#            @locations{@positions} = (0) x @positions;
#        }
#    }
    my @sorted_positions;
    if (%locations) {
        @sorted_positions = sort { $a <=> $b } keys %locations;
        ### This algo scores the position where a keyword occurs in the text.
        ### If another keyword follows closely, the position gets a higher score.
        my $limit = $excerpt_length * 2/3;
        no warnings 'uninitialized';
        foreach my $locindex (0 .. $#sorted_positions) {
            my $location = $sorted_positions[$locindex];
            my $other_locindex = $locindex - 1;
            while ($other_locindex > 0) {
                my $diff = $location - $sorted_positions[$other_locindex];
                last if $diff > $limit;
                $locations{$location} += (1/(1+log($diff)));
                --$other_locindex;
            }
            $other_locindex = $locindex + 1;
            while ($other_locindex <= $#sorted_positions) {
                my $diff = $sorted_positions[$other_locindex] - $location;
                last if $diff > $limit;
                $locations{$location} += (1/(1+log($diff)));
                ++$other_locindex;
            }
        }
        foreach (sort {$locations{$b} <=> $locations{$a}} keys %locations) {
            $best_loc = $_;
            last;
        }
    }
    else {
        $best_loc = 0;
    }
    my $textlength = length($text);
    my $start = (($best_loc - $excerpt_length) > 0) ? 
                 ($best_loc - $excerpt_length) : 0;
    my $end =   (($best_loc + $excerpt_length) < $textlength) ?
                 ($best_loc + $excerpt_length) : $textlength;
    my $substring_length = $end - $start;
    my $excerpt = substr($text, $start, $substring_length);

    $best_loc -= $start;
    my @relative_locations;
    foreach my $location (sort keys %locations) {
        my $relative_loc = $location - $start;
        next unless ($relative_loc >= 0 and
            $relative_loc < (2*$excerpt_length));
        push @relative_locations, $relative_loc;
    }
    
    my $max_chop = ((2*$excerpt_length) > $substring_length) ?
                   ($substring_length - $excerpt_length) : $excerpt_length;
    for ($excerpt) {
        my $l = length;

        ### If the best_loc is near the end, make the excerpt surround it
        if ($best_loc > 3*$l/4) {
            my $st = ($l - $excerpt_length) < 0 ?
                     ($l - $excerpt_length) : 0;
            $_ = substr($_, ($l - $excerpt_length), $excerpt_length);
            unless ($start == 0) {
                s/^\W*$tokenreg(\W*)//;
                unless (index($1, '.') > -1) {
                    $excerpt = '... ' . $excerpt;
                }
            }
            ### Remap relative locations.
            my $diff = $l - length;
            @relative_locations = sort {$a <=> $b} map {$_ - $diff} @relative_locations;
            $best_loc -= $diff;
        }

        else {
            ### Try to start the excerpt at the beginning of a sentence.
            unless ($best_loc == 0 or $start == 0) {
                s/^\W*$tokenreg//;
                unless (s/^.{0,$max_chop}\.//s) {
                    $excerpt = '... ' . $excerpt;
                }
            }
            my $diff = $l - length;
            @relative_locations = sort {$a <=> $b} map {$_ - $diff} @relative_locations;
            $best_loc -= $diff;
            ### Cut down the excerpt to no greater than the desired length.
            ### FIXME!!!! KLUDGE!!!!!
            $_ = substr($_, 0, $excerpt_length + 20);
            
            ### Clear out partial words.
            my $l2 = length;
            /($tokenreg\W*)$/;
            unless ($best_loc + length($1) eq $l2) {
                s/(\W*)$tokenreg\W*$/$1/;
            }
        }
        
        ### If the excerpt doesn't end with a full stop, end with an an ellipsis.
        unless (/\.\s*$/s) {
            s/\W+$/ .../s;
        }
    }
    ### Traverse the excerpt from back to front, inserting highlight tags.
    if ($hl_tag_open) {
        foreach my $loc (sort {$b <=> $a} @relative_locations) {
           $excerpt =~
               s/^(.{$loc})($tokenreg)/$1$hl_tag_open$2$hl_tag_close/sm;
            
        }
    }
    
    return $excerpt;
}


1;

__END__

=head1 NAME

Search::Kinosearch::KSearch - Perform searches

=head1 WARNING

Search::Kinosearch is ALPHA test software.  Aspects of the interface are
almost certain to change, given the suite's complexity.  Users should not
count on compatibility of files created by Kinosearch when future versions are
released -- any time you upgrade Kinosearch (or possibly, a module such as
L<Lingua::Stem::Snowball|Lingua::Stem::Snowball> on which Kinosearch depends),
you should expect to regenerate all kindex files from scratch. 

=head1 SYNOPSIS

    my $ksearch = Search::Kinosearch::KSearch->new(
        -mainpath => '/foo/bar/kindex',
        );
    
    my $query = Search::Kinosearch::Query->new(
        -string     => 'this AND NOT (that OR "the other thing")',
        -lowercase  => 1,
        -tokenize   => 1,
        -stem       => 1,
        );
    $ksearch->add_query( $query );
    $ksearch->process;
    
    while (my $result = $ksearch->fetch_hit_hashref) {
        print "$result->{title}\n";
    }

=head1 DESCRIPTION

KSearch objects perform queries against the kindex files created by Kindexer
objects.  

Queries are fed into KSearch using
L<Search::Kinosearch::Query|Search::Kinosearch::Query> objects.
You can feed multiple Query objects to a KSearch object in order to fine tune
your result set, but KSearch objects themselves are single shot -- if you need
to perform multiple searches, you need to create multiple objects.  

=head2 Multiple calls to add_query()

It is possible to perform a search which is the result of multiple queries -
in fact, that is the only way to implement an "advanced search" interface:

    my $find_the_word_people = Search::Kinosearch::Query->new(
        -string     => 'people',
        -required   => 1,
        -fields     => {
                            title    => 3,
                            bodytext => 1,
                       },
        -tokenize   => 1,
        -stem       => 1,
        -lowercase  => 1,
        );
    my $in_article_ii_only = Search::Kinosearch::Query->new(
        -string => 'Article II',
        -required   => 1,
        -fields => {
                       section    => 1,
                   },
        );
    $ksearch->add_query( $find_the_word_people );
    $ksearch->add_query( $in_article_ii_only   );
    my $status = $ksearch->process; 
    ...

Since both queries are marked as '-required => 1', all documents returned must
1) match 'people' in one or both of the 'title' and 'bodytext' fields, and 2)
match 'Article II' in the 'section' field.

=head2 Excerpts

Kinosearch attempts to find the section of the text with the greatest density
of search terms in a field that you specify (typically the bodytext).  Any
search terms encountered within the text are highlighted with html tags.  In
addition to the field from which the excerpt is taken, Kinosearch gives you
control over the length of the the excerpt and the text of the highlight tags.

=head1 CONSTRUCTOR

=head2 new()

    my $ksearch = Search::Kinosearch::KSearch->new(
        -mainpath          => '/foo/kindex' # default: 'kindex'
        -freqpath          => '/ramd/fdata' # default: 'kindex/freqdata'
        -kindex            => $kindex,      # default: created using -mainpath
        -any_or_all        => 'any',        # default: 'any'
        -sort_by           => 'score',      # default: 'string'
        -allow_boolean     => 0,            # default: 1
        -allow_phrases     => 0,            # default: 1
        -num_results       => 20,           # default: 10
        -offset            => 40,           # default: 0
       #-language          => 'Es',         # default: 'En'
        -stoplist          => \%big_list    # default: see below
        -excerpt_field     => 'bodytext',   # default: undef
        -excerpt_length    => 200,          # default: 150
        -hl_tag_open       => '<b>',        # default: '<strong>'
        -hl_tag_close      => '</b>',       # default: '</strong>'
        );

Construct a KSearch object.

=over

=item -mainpath

The path to your kindex.

=item -freqpath

Specify an alternative location for the frequency data -- most likely, a ram
disk.  

=item -kindex

A L<Search::Kinosearch::Kindex|Search::Kinosearch::Kindex> object.  If you
provide such an object, you don't need to specify -mainpath or -freqpath.

=item -any_or_all

Searches return results containing 'any' or 'all' search terms.  

=item -sort_by

'score' or 'datetime'.

=item -allow_boolean

If set to 0, disables parenthetical groupings; boolean terms "AND", "OR" and
"AND NOT"; and prepended +plus and -minus.

=item -allow_phrases

Enable/disable phrase-matching.

=begin comment

=item -max_terms

Maximum allowed terms in a search query.  If set to 2, for example, then a
search for 'foo bar baz' ignores 'baz' and returns results relevant only to
'foo bar'.  DEVNOTE: disabled.

=end comment

=item -num_results

Maximum number of documents returned.

=item -offset

Number of documents to skip when returning ranked results.  Example: if
-offset is set to 10, the first document returned will be the 11th most highly
ranked. 

=item -language

The language of the query.  At present only 'En' works.  See
L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua>.

=item -stoplist

A hashref of words to exclude from the query.  If no list is specified, a
default list is loaded based on the -language parameter; for instance, if
-language is set to 'Es', then $Search::Kinosearch::Lingua::Es::stoplist
is used.  Stopwords encountered in the query are reported in the search status
hash returned by process().

=item -excerpt_field

Field to be used when generating excerpts.

=item -excerpt_length

Maximum length of excerpt, in characters.

=item -hl_tag_open

Override the default opening tag used to highlight search terms which appear in the excerpt.

=item -hl_tag_close

Override the default closing tag used to highlight search terms which appear in the excerpt.  

=back

=head1 METHODS

=head2 add_query()

    $ksearch->add_query( $query )

Add a query, in the form of a Search::Kinosearch::Query object, to the 
KSearch object.  

=head2 process()

    my $searchstatus = $ksearch->process;
    print "Documents matched: $searchstatus->{num_hits}\n";

Execute the search, generate a result list, and return a hashref pointing to
information about the search.

Here's how the status hash might look if you were to search for 'we the people
in order to form a more perfect union'. 

    $searchstatus = {
        num_docs            => 52
        num_hits            => 18,       
        stopwords           => {
            we  => undef,
            the => undef,
            in  => undef,
            to  => undef,
            a   => undef,
            },
        };

=over   

=item num_docs

The number of documents searched.

=item num_hits

The approximate number of documents matched.  (The number is only approximate
because it may include documents which have been marked as deleted, but not
yet purged from the kindex.)

=item stopwords

A hash where the keys are stopwords encountered. 

=back

=head2 fetch_hit_hashref()

Shift ranked results off of an array.  Each result is a hashref with all
stored fields represented.  Two special fields are added.

=over 

=item excerpt

A relevant excerpt taken from the field specified by the -excerpt_field
parameter.

=item score

The document's numerical score.

=back

=head1 TO DO

=over

=item

Think hard about the interface, specifically about all the parameters
supplied to the constructor.  If KSearch gets broken into smaller pieces, 
those parameters should go away.  Better to do that soon, while the
user base is small.

=item

Break out excerpting/highlighting code into a separate module.

=item

Sanity check: process can only be called once.

=back

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch|Search::Kinosearch>

=item

L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer>

=item

L<Search::Kinosearch::Query|Search::Kinosearch::Query>

=item

L<Search::Kinosearch::Kindex|Search::Kinosearch::Kindex>

=item

L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua>

=item

L<Search::Kinosearch::Tutorial|Search::Kinosearch::Tutorial>

=back

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut


