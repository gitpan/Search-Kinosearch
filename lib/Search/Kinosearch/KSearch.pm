package Search::Kinosearch::KSearch;
use strict;
use warnings;

use base qw( Search::Kinosearch Search::Kinosearch::QueryParser
             Class::WhiteHole);
use Search::Kinosearch::Kindex;
use Search::Kinosearch::Query;
use Search::Kinosearch::Doc;

use attributes 'reftype';
use bytes;

use Carp;

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
    result_set         => undef, # defined later...
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
    
    ### TODO separate QueryParser from KSearch completely.
    $self->_init_queryparser;
    
    ### Define tokenizing, stemming, $tokenreg, stoplist, etc.
    $self->define_language_specific_functions;

    ### Assign a range for document numbers to each subkindex.
    my $doc_num_offset = 0;
    $self->{subk_doc_num_offsets} = [];
    for (0 .. $#{ $self->{kindex}{subk_docs} }) {
        $self->{subk_doc_num_offsets}[$_] = $doc_num_offset;
        $doc_num_offset += $self->{kindex}{subk_docs}[$_];
    }
#    $self->{result_set} = Search::Kinosearch::KSearch::ResultSet->new(0,0);
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

    my $kindex = $self->{kindex};

    my $kinodel = $kindex->{kinodel};
    my $subkindexes = $kindex->{subkindexes};

    $self->parse;

    $self->_recursive_score($self);

    my $result_set = $self->{result_set};
    
    my $total_hits = $result_set->_get_num_hits;
    $result_set->_sort_hits($self->{-sort_by});
    my @ranked_results;
    $self->{ranked_results} = \@ranked_results;
    $self->{status_hash}{num_docs} = $kindex->{num_docs};
    my $num_hits = $self->{status_hash}{num_hits} = $result_set->_get_num_hits;
    return $self->{status_hash} unless $num_hits;
    
    my $first = $self->{-offset};
    my $last = $first + $self->{-num_results};
    my $valid_hits = 0;
    
    my $hit_iter = 0;
    my ($doc_num, $raw_score);

    ### Get the highest raw score in the bunch, so that we can normalize all
    ### scores.
    my $hit_info = $result_set->_retrieve_hit_info($hit_iter); 
    my $highest_raw_score = $hit_info->{score} ? $hit_info->{score} : 1;

    $hit_iter = -1;
    ### Assemble a hit list.
    my $descending = $self->{-ascdesc} eq 'descending' ? 1 : 0;
    my $storable_fields = $kindex->{fields_store};
    my $num_storable_fields = @$storable_fields;
    my $subk_doc_num_offsets = $self->{subk_doc_num_offsets};
    while ($valid_hits < $last and $hit_iter < ($num_hits-1)) {
        $hit_iter++;
        my $raw_hit_num = $descending ? $hit_iter : $total_hits - $hit_iter;
        $hit_info = $result_set->_retrieve_hit_info($raw_hit_num);
        $doc_num = $hit_info->{doc_num};
        $raw_score = $hit_info->{score};
        my $knum = -1; 
        for (@$subk_doc_num_offsets) {
            $knum++;
            next unless $_ >= $doc_num;
            $doc_num -= $_;
            last;
        }
        next if exists 
            $kinodel->{$doc_num}{$knum};
        $highest_raw_score ||= $raw_score;
        ### If it isn't a deletion, it's a valid hit.
        $valid_hits++;
        ### If we haven't reached the offset yet, go to the next hit.
        next unless $valid_hits > $first;
        ### Retrieve stored document fields, and put them in a Doc object.
        my $docdata = $subkindexes->[$knum]{kinodocs}[$doc_num];
        my %stored_fields;
        
        my @field_lengths 
            = unpack('N*', substr($docdata, 36, ($num_storable_fields*4)));
        my $len_stored = 36 + $num_storable_fields*4;
        $len_stored += $_ for @field_lengths;

        my $stored_fields_tpt = 'a36 a' . ($num_storable_fields*4) . ' ';
        $stored_fields_tpt .= "a$_ " for @field_lengths;
        (undef, undef, @stored_fields{@$storable_fields}) = 
            unpack($stored_fields_tpt, substr($docdata, 0, $len_stored, ''));

        my ($num_tokens, $num_tokens_bigger_than_250) 
            = unpack('N N ', substr($docdata, 0, 8, ''));
        my @posdeltas;
        if ($num_tokens_bigger_than_250) {
            @posdeltas 
                = unpack('C*', substr($docdata, 0, ($num_tokens + 8),''));
            my @bigger_than_250 = unpack('n*', $docdata);
            for (@posdeltas) {
                $_ = shift @bigger_than_250 if $_ == 255;
            }
        }
        else {
            @posdeltas = unpack('C*', $docdata);
        }
        
        my @positions;
        my $pos = 0;
        for (@posdeltas) {
            $pos += $_;
            push @positions, $pos;
        }
        my $token_positions = $hit_info->{positions};
        @positions = @positions[@$token_positions];

        my ($excerpt_field_start, $excerpt_field_length) = (0,0);
        for (0 .. $#$storable_fields) {
            last unless defined $self->{-excerpt_field};
            if ($self->{-excerpt_field} eq $storable_fields->[$_]) {
                $excerpt_field_length = $field_lengths[$_];
                last;
            }
            else {
                $excerpt_field_start += $field_lengths[$_];
            }
        }
        my $excerpt_field_end = $excerpt_field_start + $excerpt_field_length;
        @positions = grep 
            { $_ >= $excerpt_field_start and $_ < $excerpt_field_end }
                @positions;
        $_ -= $excerpt_field_start for @positions;    

        my $doc = Search::Kinosearch::Doc->new( 
            { fields => \%stored_fields } );
        if ($self->{-sort_by} eq 'score') {    
            $doc->set_field( score =>  $raw_score/$highest_raw_score);
        }
        push @ranked_results, $doc;
        if ($self->{-excerpt_field}) {
            $doc->set_field( 
                excerpt => $self->_create_excerpt($knum, $doc, \@positions) );
        }
    }
    return $self->{status_hash};
}

### Like many recursive routines, the flow of the _recursive_score 
### subroutine is difficult to follow.  This verbose explanation is 
### intended to compensate for the fact that even heavily commented, the code
### doesn't do a good job of documenting itself.
### 
### The tree which _recursive_score walks is itself the product of
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

    if ($sprout->{prodtype} eq 'terminal') {
        $self->_retrieve_term_scores($sprout);
    }
    
    return unless $sprout->{productions} and @{ $sprout->{productions} };

    ### Sort productions into the order which yields the quickest set mergers.
    my @sorted_productions = sort {
        $b->{negated}  <=> $a->{negated}
                       or
        $b->{required} <=> $a->{required}
                       or
        $a->{result_set}->_get_num_hits <=> $b->{result_set}->_get_num_hits
        } (@{ $sprout->{productions} });

    $self->_recursive_score($_) foreach @sorted_productions;
     
    if ($sprout->{prodtype} eq 'phrase' 
        and @{ $sprout->{productions} } > 1) 
    {
        ### Work backwards from the end of the phrase, winnowing down the
        ### docs.  If the phrase to be matched is "I am the walrus", match 
        ### "the" and "walrus" in successive token positions, producing a 
        ### result set consisting of documents which match "the walrus" and
        ### positional data for where "the walrus" starts.  Armed with
        ### this result, look for documents which contain "am" followed by 
        ### "the walrus"... and so on... In the end, we will have a result 
        ### set consisting of documents which contain the full phrase "I 
        ### am the walrus" and markers pointing to where the phrase begins.
        for (reverse (1 .. $#{ $sprout->{productions} })) {
            my $s1 = $sprout->{productions}[$_ - 1]{result_set};   
            my $s2 = $sprout->{productions}[$_]{result_set};   
            $s1->_score_phrases($s2);
        }
        
        $sprout->{result_set} = $sprout->{productions}[0]{result_set};
        my $phraselength = @{ $sprout->{productions} };
        $sprout->{result_set}->_expand_phrase_posdata($phraselength);
        return;
    }
    
    my $reqlist = undef;
    my $neglist = undef;
    my $result_set = Search::Kinosearch::KSearch::ResultSet->new(0,0);
    
    ### If the parent node has only one child, then inheritance is
    ### straightforward -- the result set passes from parent to child
    ### without any modification.
    if (@{ $sprout->{productions} } == 1){
        if ($sprout->{productions}[0]{result_set}->_get_num_hits) {
            if ($sprout->{productions}[0]{negated}) {
                ### an *empty* result set.
                $sprout->{result_set} = $result_set;
            }
            else {
                $sprout->{result_set} = $sprout->{productions}[0]{result_set};
            }
            return;
        }
    }
    
    ### Combine the reqlists and neglists for all the productions.  
    ### We do this first so we can filter before we score.
    PRODUCTION: foreach my $production (@sorted_productions) {
        next PRODUCTION if $production->{prodtype} eq 'boolop';
        my $prod_result_set = $production->{result_set};
        if ($production->{negated}) {
            $neglist = 
                $prod_result_set->_modify_filterlist($neglist, 'union');
            next PRODUCTION;
        }
        ### If there's no neglist, no reqlist, and no existing result_set...
        ### simply assign the production's result_set to the aggregate
        ### result_set, and if the production is required, dupe its doc_num
        ### list into the reqlist.
        if  ((!defined $neglist) and (!defined $reqlist) 
                and !$result_set->_get_num_hits) 
        {
            # The if clause shouldn't have been necessary but it
            # fixed a crashing bug. 
            $result_set = $prod_result_set 
                if ($prod_result_set and $prod_result_set->_get_num_hits);
            
            if ($production->{required}) {
                $reqlist = $prod_result_set->_modify_filterlist($reqlist, 'union');
            }
            next PRODUCTION;
        }
        
        ### Eliminate negated documents.
        if (defined $neglist) {
            $prod_result_set->_apply_filterlist($neglist, 'neg');
        }
        
        ### If there's a pre-existing list of required documents, apply it as
        ### mask so that the production's result set only includes docs which
        ### are on the reqlist.
        if (defined $reqlist) {
            
            $prod_result_set->_apply_filterlist($reqlist, 'req');
        }
        if (defined $reqlist or defined $neglist) {
            $prod_result_set->_filter_zero_scores;
        }
        
        if ($production->{required}) {
            if (defined $reqlist) {
                $reqlist = $prod_result_set->_modify_filterlist(
                    $reqlist, 'intersection');
            }
            else {
                $reqlist = $prod_result_set->_modify_filterlist(
                    $reqlist, 'union');
            }
            if ($result_set->_get_num_hits) {
                $result_set->_apply_filterlist($reqlist, 'req');
                $result_set->_filter_zero_scores;
            }
        }
        if ($result_set->_get_num_hits) {
            my $nh = $result_set->_get_num_hits
                + $prod_result_set->_get_num_hits;
            my $np = $result_set->_get_num_pos
                + $prod_result_set->_get_num_pos;
            my $rs = Search::Kinosearch::KSearch::ResultSet->new($nh, $np);   
            my $retval = $result_set->_merge_result_sets($prod_result_set, $rs);
            $result_set = $retval == 0 ? $rs :
                          $retval == 1 ? $result_set : 
                          $prod_result_set;
            
        }
        else {
            $result_set = $prod_result_set;
        }
#        $result_set = $result_set->_get_num_hits ?
#                      $result_set->_merge_result_sets($prod_result_set) :
#                      $prod_result_set;
    }
    $sprout->{result_set} = $result_set;
}

sub _retrieve_term_scores {
    my $self = shift;
    my $sprout = shift;

    my $kindex = $self->{kindex};
    my $term = $sprout->{qstring};

    my $total_hits = 0;
    my $total_posdata_len = 0;
    for my $knum (0 .. $#{ $kindex->{subkindexes} }) {
        my $doc_num_offset = $self->{subk_doc_num_offsets}[$knum];
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
            @entry{ qw(doc_freq offset filenum 
                       posdata_length posdata_offset posdata_filenum) }
                = unpack('N*', $subkindex->{kinoterms}{$term});
            $sprout->{kinoterms_entries}[$knum] = \%entry;
            $total_hits += $entry{doc_freq};
            $total_posdata_len += $entry{posdata_length};
        }
    }

    my $result_set = Search::Kinosearch::KSearch::ResultSet->new(
        $total_hits, $total_posdata_len);
    $sprout->{result_set} = $result_set;
    return if $total_hits == 0;

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
    my $idf  = log(($num_docs+1)/$total_hits);
    
    ### Prepare the multipliers for each field we're going to score on.
    ### We'd have to multiply each hit's score by idf anyway.  It's more
    ### efficient to prepare a single multiplier consisting of weight * idf
    ### than it is to multiply each result by weight and idf separately.
    ###
    ### For even more efficiency in the C scoring routines, we convert to an 
    ### integer to allow integer math instead of float... and multiply by 10
    ### to minimize truncation errors.
    my %factors;
    while (my ($field, $weight) = each %scorefields) {
        $factors{$field} = int($idf * $weight * 10) + 1;
        croak("Can't score field '$field'") 
            if ($kindex->{kdata_bytes}{$field} != 2 
                and $self->{-sort_by} eq 'score');
    }
        
    my ($previous_hits, $previous_positions) = (0,0);
    SUBKINDEX: for my $knum (0 .. $#{ $kindex->{subkindexes} }) {
        next unless (exists $sprout->{kinoterms_entries}[$knum]
            and defined $sprout->{kinoterms_entries}[$knum]);
        my $subkindex = $kindex->{subkindexes}[$knum];
        my $doc_num_offset = $self->{subk_doc_num_offsets}[$knum];
        my $kinoterms_entry = $sprout->{kinoterms_entries}[$knum];
        
        my $packed_kinodata_ref;
        ### Read the relevant sections of kinodata into the ResultSet object.
        $packed_kinodata_ref = $kindex->_read_kinodata( $subkindex, 'doc_num', 
            @{ $kinoterms_entry }{'filenum','offset','doc_freq'});
        $result_set->_set_KDfield_str('doc_num', $packed_kinodata_ref, 
            $previous_hits, $doc_num_offset, 4, 1);

        $packed_kinodata_ref = $kindex->_read_kinodata( $subkindex, 'posaddr',
            @{ $kinoterms_entry }{'filenum','offset','doc_freq'});
        $result_set->_set_KDfield_str('posaddr', $packed_kinodata_ref, 
            $previous_hits, 0, 2, 1);
        
        $packed_kinodata_ref = $kindex->_read_kinodata( $subkindex, 'posdata',
            @{ $kinoterms_entry }{'posdata_filenum','posdata_offset',
                'posdata_length'});
        $result_set->_set_KDfield_str('posdata', $packed_kinodata_ref, 
            $previous_hits, 0, 4, 1);
        #$result_set->_set_posdata($packed_kinodata_ref, $previous_positions);
        
        if ($kindex->{datetime_enabled}) {
            $packed_kinodata_ref = $kindex->_read_kinodata( 
                $subkindex, 'datetime', 
                @{ $kinoterms_entry }{'filenum','offset','doc_freq'});
            my @stuff = unpack('C*', $$packed_kinodata_ref);
            $result_set->_set_KDfield_str('datetime', $packed_kinodata_ref, 
                $previous_hits, 0, 8, 0);
        }
        
        for my $scorefield (@scorefields_arr) {
            $packed_kinodata_ref = 
                $kindex->_read_kinodata( $subkindex, $scorefield,
                    @{ $kinoterms_entry }{'filenum','offset','doc_freq'});
            $result_set->_add_up_scores($packed_kinodata_ref,
                $factors{$scorefield}, $previous_hits);
        }
        
        $previous_hits += $kinoterms_entry->{doc_freq};
        $previous_positions += $kinoterms_entry->{posdata_length};
    }
    ### We only have to filter if -fields has been specified.
    if ($self->{-sort_by} eq 'score' 
        and ref $sprout->{-fields} 
        and %{ $sprout->{-fields} }) 
    {
        $result_set->_filter_zero_scores;
    }
    $sprout->{result_set} = $result_set;
}

sub fetch_hit_hashref {
    my $self = shift;
    if (my $ranked_result =  shift @{ $self->{ranked_results} }) {
        return $ranked_result->{fields};
    }
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
        last if $_ eq $fieldname;
        $fieldnumber++;
    }

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
                   #($substring_length - $excerpt_length) : $excerpt_length;
                   ($excerpt_length - $substring_length) : $excerpt_length;
    for ($excerpt) {
        my $l = length;

        ### If the best_loc is near the end, make the excerpt surround it
        if ($best_loc > 3*$l/4) {
            my $st = ($l - $excerpt_length) > 0 ?
                     ($l - $excerpt_length) : 0;
            $_ = substr($_, $st, $excerpt_length);
            unless ($start == 0) {
                no bytes;
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
                no bytes;
                s/^\W*$tokenreg//;
                unless (s/^.{0,$max_chop}\.//s) { 
                    $excerpt = '... ' . $excerpt;  # vim highlighting hack: )
                }
            }
            my $diff = $l - length;
            @relative_locations = sort {$a <=> $b} map {$_ - $diff} @relative_locations;
            $best_loc -= $diff;
            ### Cut down the excerpt to no greater than the desired length.
            ### FIXME!!!! KLUDGE!!!!!
            $_ = substr($_, 0, $excerpt_length + 20);
            
            ### Clear out partial words.
            {
                no bytes;
                if (/$tokenreg$/) {
                    s/(\W*)$tokenreg$/$1/;
                }
            }
        }
        
        ### If the excerpt doesn't end with a full stop, end with an an ellipsis.
        no bytes;
        unless (/\.\s*$/s) {
            s/\W+$/ .../s;
        }
    }
    ### Traverse the excerpt from back to front, inserting highlight tags.
    if ($hl_tag_open) {
        foreach my $loc (sort {$b <=> $a} @relative_locations) {
            $excerpt =~
               s/^(\C{$loc})($tokenreg)/$1$hl_tag_open$2$hl_tag_close/sm;
            
        }
    }
    
    return $excerpt;
}

1;

__END__
__POD__

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

The document's numerical score.  This will be 1 for the top hit and between 0
and 1 for all other hits.

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


