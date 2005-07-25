package Search::Kinosearch::Kindexer;
use strict;
use warnings;

use base qw( Search::Kinosearch Search::Kinosearch::Kindex );
use Search::Kinosearch::Doc;

use Sort::External;
use Carp;
use Digest::MD5 qw( md5_hex );
use String::CRC32 'crc32';
use File::Spec;
use File::Copy qw( copy move );
use File::Temp qw( tempdir );
use Fcntl qw(:DEFAULT :flock);
use Storable qw( nfreeze thaw nstore );

my $tempdir_template = "kinotemp_XXXX";

### Build the default path for the kindex output directory.
my $current_directory = File::Spec->curdir();
my $default_mainpath   = File::Spec->catdir( $current_directory, 'kindex' );
$default_mainpath = File::Spec->rel2abs($default_mainpath);

### Don't distinguish developer versions.
my ($kinosearch_version) = ($Search::Kinosearch::VERSION =~ /(\d+\.\d\d)/);

##############################################################################
### Constructor
##############################################################################
sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->init_kindexer(@_);
    return $self;
}

my %init_kindexer_defaults = (
    -mode                   => 'update',
    -mainpath               => $default_mainpath,
    -freqpath               => undef,   # will be defined...
    ### -temp_directory is the holder for the *real* temp directory.
    -temp_directory         => $current_directory, 
    -language               => 'En',
    -encoding               => 'UTF-8',
    -phrase_matching        => 1,
    -enable_datetime        => 0,
    -enable_sortstring      => 0,
    -stoplist               => undef,   # defined later
    -max_kinodata_fs        => 2 ** 28, # 256 Mb
    -optimization           => 2,
    -verbosity              => 0,
    version                 => $kinosearch_version,
    default_sortstring      => '',
    sortstring_bytes        => 0,
    datetime_enabled        => 0,
    datetime_string         => '',
    );

##############################################################################
### Initialize a Search::Kinosearch::Kindexer object.
##############################################################################
sub init_kindexer {
    my $self = shift;
    
    %$self = (%init_kindexer_defaults, %$self);
    while (@_) {
        my ($var, $val) = (shift, shift);
        croak("Invalid parameter: '$var'")
            unless exists $init_kindexer_defaults{$var};
        $self->{$var} = $val;
    }
    croak("Invalid value for parameter -mode: '$self->{-mode}'")
        if $self->{-mode} !~ /^(?:overwrite|update)$/;
        
    ### Make the Kindexer object a Search::Kinosearch::Kindex.
    $self->init_kindex;
    my $subkindexes = $self->{subkindexes};

    ### Establish a write lock on the entire kindex.  This isn't a traditional
    ### write lock, though.  All kindex modifications happen in a temp
    ### directory and are move()d at the very last minute.  While the
    ### modifications are being calculated, it is still safe to perform 
    ### searches -- however, it is _not_ safe to start another independent 
    ### update process, and that's what this write lock prevents.
    my $writelockpath = $self->{kinowritelock}{path} = File::Spec->catfile(
        $self->{mainpath}, 'kinowritelock');
    my $writelockfh;
    my $writelockflags =  -e $writelockpath ?
        O_RDONLY : (O_WRONLY | O_CREAT | O_EXCL);
    sysopen $writelockfh, $writelockpath, $writelockflags 
        or croak "Couldn't open lockfile '$writelockpath': $!";
    print "Waiting for writelock..." if $self->{-verbosity};
    flock($writelockfh, LOCK_EX | LOCK_NB )
        or croak ("Couldn't establish exclusive lock " .
                  "on lockfile '$writelockpath': $!");
    print "got it.\n" if $self->{-verbosity};
    $self->{kinowritelock}{fh} = $writelockfh;

    ### Define tokenizing, stemming, tokenreg, etc...
    $self->{language} = $self->{-language};
    $self->define_language_specific_functions;
    
    ### Create a temp directory
    $self->{temp_dir} = tempdir(
        $tempdir_template,
        DIR     => $self->{-temp_directory},
        CLEANUP => 1,
        );
    
    ### Survey the number of documents in each subkindex.
    my @subk_docs;
    push @subk_docs, (scalar keys %{ $_->{kinodocs} }) for @$subkindexes;

    ### Create the outkindex.
    ### All writes are performed on the outkindex, which is stored in 
    ### the temp directory until write_kindex() is called.
    my $optimization = $self->{optimization} = $self->{-optimization};
    my $outknum;
    my @to_consolidate;
    my $outkindex_mode = 'create';
    if ($self->{-mode} eq 'update' and @$subkindexes) {
        ### Under optimization levels 1, 2, or 3 it may make sense to start 
        ### off the outkindex as a copy of one of the subkindexes.
        if ($self->{optimization} == 1) {
            $outknum = 0;
            @to_consolidate = (0 .. $#$subkindexes);
        }
        elsif ($self->{optimization} == 2) {
            if (@$subkindexes < 2) {
                # Just add a second subkindex... 
            }
            elsif ((@$subkindexes > 2) 
                    or ($subk_docs[1] > $subk_docs[0]/10)) 
            {
                ### Either... the secondary subkindex has gotten too big, 
                ### time to merge... or... there aren't 2 subkindexes.
                $outknum = 0;
                @to_consolidate = (0 .. $#$subkindexes);
            }
            else {
                ### The secondary subkindex is still small enough that we'll
                ### add to it and leave the primary subkindex be.
                $outknum = 1;
                @to_consolidate = (1 .. $#$subkindexes)
                    if @$subkindexes > 1;
            }
        }
        elsif ($optimization == 3) {
            ### start by assuming we'll need to consolidate all subkindexes,
            ### then pare down the list.
            @to_consolidate = (0 .. $#subk_docs);
            for my $knum (0 .. $#subk_docs) {
                if (@to_consolidate < 10) {
                    @to_consolidate = ();
                    last;
                }
                my $big = $subk_docs[$knum];
                my $small;
                $small += $subk_docs[$_] for ($knum .. ($knum + 9));
                last if $small > $big;
                shift @to_consolidate;
            }
            $outknum = $to_consolidate[0]
                if @to_consolidate;
        }
    }
    if (defined $outknum) {
        $outkindex_mode = 'update';
    }
    else {
        $outkindex_mode = 'create';
        $outknum = @$subkindexes ? @$subkindexes : 0;
    }
    my $mpath = File::Spec->catdir($self->{temp_dir}, "subkindex$outknum");
    mkdir $mpath or croak("Couldn't create directory '$mpath': $!");
    my $fpath = File::Spec->catdir($self->{temp_dir}, "freqdata$outknum");
    mkdir $fpath or croak("Couldn't create directory '$fpath': $!");
    if ($outkindex_mode eq 'update') {
        my $sourcepath = $subkindexes->[$outknum]{mpath};
        opendir SOURCEDIR, $sourcepath
            or croak("Couldn't read directory '$sourcepath': $!");
        my @files = grep { /^kino/ } readdir SOURCEDIR;
        closedir SOURCEDIR;
        my @destfiles = map { File::Spec->catfile($mpath, $_) } @files;
        @files = map { File::Spec->catfile($sourcepath, $_) } @files;
        copy($files[$_], $destfiles[$_]) for (0 .. $#files);
        ### kinoterms hash is stored in the freqdata directory
        my $kinoterms_sourcepath = File::Spec->catfile(
            $subkindexes->[$outknum]{fpath}, 'kinoterms');
        my $kinoterms_destpath = File::Spec->catfile(
            $fpath, 'kinoterms');
        copy($kinoterms_sourcepath, $kinoterms_destpath);
    } 
    $self->{outknum} = $outknum;
    ### We won't consolidate until generate(), in case of deletions or
    ### modifications...
    $self->{to_consolidate} = \@to_consolidate;
    
    my $outkindex = $self->{outkindex} 
        = Search::Kinosearch::SubKindex->new(
            mode                   => $outkindex_mode,
            mpath                  => $mpath,
            fpath                  => $fpath,
            );
    if ($outkindex_mode eq 'update') {
        $self->{subkindexes}[$outknum] =  $outkindex;
    }
    else {
        push @$subkindexes, $outkindex;
    }
    
    my $kinodel = $self->{kinodel};
    
    ### Find the lowest doc_num in the outkindex.
    my $highest = scalar keys %{ $outkindex->{kinodocs} };
    if ($highest) {
        while (1) {
            $highest++;
            next if exists $outkindex->{kinodocs}{$highest};
            next if (exists $kinodel->{$highest} 
                     and exists $kinodel->{$highest}{$outknum});
            last;
        }
    }
    $self->{doc_num} = $highest;

    if (!$self->{field_defs}) { 
        ### If there aren't any field definitions yet, that means kinodata 
        ### wasn't read and this is a brand new kindex.
        ### Define a few fields which will be required for internal use.
        $self->define_field(
            -name            => 'doc_id',
            -store           => 1,
            -score           => 0,
            );
        $self->define_field(
            -name            => 'doc_mdfive',
            -store           => 1,
            -score           => 0,
            );
        $self->define_field(
            -name            => 'doc_num',
            -score           => 0,
            -kdata_bytes     => 4,
            );
    }
    
    if ($self->{-enable_datetime} or $self->{datetime_enabled}) {
        $self->{datetime_enabled} = 1;
        $self->{kdata_bytes}{datetime} = 8;
    }
                                       
    ### Enable the sortstring (used in range queries).  EXPERIMENTAL!
    if ($self->{-enable_sortstring} or $self->{sortstring_bytes}) {
        my $bytes = $self->{-enable_sortstring} || $self->{sortstring_bytes};
        $self->{sortstring_bytes} = $bytes;
        $self->{kdata_bytes}{sortstring} = $bytes;
        $self->{default_sortstring} = "\0" x $bytes;
    }
                                       
    ### Unless we're in update mode (in which case num_docs was set by
    ### init_kindex), set num_docs to 0.
    $self->{num_docs} ||= 0;
    
    ### The Sort::External object autosorts the kinodata cache.
    $self->{kinodata_cache} = Sort::External->new(
        -working_dir => $self->{temp_dir},
        #-cache_size  => ??? TODO
        -line_separator => 'random',
        );
}

##############################################################################
### Spawn a new Search::Kinosearch::Doc object.
##############################################################################
sub new_doc {
    my $self = shift;
    unless (@_ == 1) {
        croak("Expecting one argument: a unique document identifier");
    }
    my $doc_id = shift;
    
    my %fields;
    $fields{$_} = undef for @{ $self->{fields_all} };
    $fields{doc_id} = $doc_id;

    my %args_to_doc;
    
    if ($self->{datetime_enabled}) {
        %args_to_doc = ( 
            fields => \%fields,
            sortstring => $self->{default_sortstring},
            datetime_ymdhms => [ 0,0,0,0,0,0 ],
            datetime_string => "\0\0\0\0\0\0\0\0",
            );
    }
    else {
        %args_to_doc = ( 
            fields => \%fields,
            sortstring => $self->{default_sortstring},
            datetime_ymdhms => undef,
            datetime_string => '', 
            );
    }

    my $doc = Search::Kinosearch::Doc->new( \%args_to_doc );

    return $doc;
}

##############################################################################
### Add a document to the kindex.
##############################################################################
sub add_doc {
    my $self = shift;
    my $doc = shift or croak("Expecting a Search::Kinosearch::Doc object");
    
    my $docfields = $doc->{fields};
    
    $self->{initialized} ||= 1;
    
    my $doc_num = $self->{doc_num};
    $doc->set_field( doc_num => $doc_num );
    
    ### Prepare the doc_mdfive, a hash value which will be used to identify 
    ### the doc in various places.
    my $doc_id = $doc->get_field('doc_id');
    my $doc_mdfive = md5_hex($doc_id);
    $doc->set_field( doc_mdfive => $doc_mdfive );
    
    ### If a document with the same unique id already exists in the kindex,
    ### overwrite it.
    $self->delete_doc( -doc_mdfive => $doc_mdfive );
    
    my (%to_score, %unstemmed, %positions);
    my $tokenize = $self->{tokenize_method};
    my $stem = $self->{stem_method};
    my $field_defs = $self->{field_defs};
    
    for my $field (@{ $self->{fields_all} }) {
        $to_score{$field} = [];
        $to_score{$field}[0] = $field_defs->{$field}{-lowercase} ?
            lc($docfields->{$field}) : $docfields->{$field};

        if ($field_defs->{$field}{-tokenize}) {
            ($to_score{$field}, $positions{$field}) 
                = $self->$tokenize($to_score{$field}[0]);
        }
        else {
            $positions{$field} = [];
        }

        if ($field_defs->{$field}{-stem}) {
            $unstemmed{$field} = $to_score{$field};
            $to_score{$field} = $self->$stem($to_score{$field});
        }
    }

    if ($self->{sortstring_bytes}) {
        my $sortstring_bytes = $self->{sortstring_bytes};
        my $sortstring = $doc->{sortstring};
        ### sortstring is fixed length, so null-pad or truncate it as necessary.
        use bytes;
        my $len = length($sortstring);
        if ($len == $sortstring_bytes) {
            ;
        }
        elsif ($len < $sortstring_bytes) {
            $sortstring .= "\0" x ($sortstring_bytes - $len);
        }
        elsif ($len > $sortstring_bytes) {
            $sortstring = substr($sortstring, 0, $sortstring_bytes);
        }
        $len = length($sortstring);
        $doc->{sortstring} = $sortstring;
    }
    
    ### Produce proximity data
    my $storable_fields = $self->{fields_store};
    my @field_lengths = map { length $docfields->{$_} } @$storable_fields;
    my $field_lengths_string = '';
    $field_lengths_string .= pack('N', $_) for @field_lengths;
    my $posit_offset = 0;
    my %token_infos;
    for my $storable_field (@$storable_fields) {
        my $posits = $positions{$storable_field};
        no warnings 'uninitialized';
        for my $token (@{ $to_score{$storable_field} }) {
            $token_infos{ pack('N',(crc32($token) )) } 
                .= pack( 'N', ((shift @$posits) + $posit_offset) );
        }
        $posit_offset += shift @field_lengths;
    }
    
    my $num_tokens = pack('N', scalar keys %token_infos);
    my @token_info_lengths;
    {
        use bytes;
        @token_info_lengths = map { length($_) } values %token_infos;
    }

    $self->{outkindex}{kinodocs}{$doc_num} = 
        $field_lengths_string . 
        (join '', @{ $docfields }{ @$storable_fields }) .
        $num_tokens . 
        (join '', keys %token_infos) .
        pack ('N*', @token_info_lengths) . 
        (join '', values %token_infos );
        
    $self->{outkindex}{kinoids}{$doc_mdfive} = $doc_num;
    $self->_score_terms($doc,$doc_num,\%to_score,\%unstemmed );

    $self->{doc_num} += 1;
    $self->{num_docs} += 1;
}

##############################################################################
### Delete a document from the kindex.
### Note that it isn't _really_ deleted until finish() completes.
##############################################################################
sub delete_doc {
    my $self = shift;
    my ($doc_id, $doc_mdfive);
    if (@_ > 1) {
        croak "Expecting one argument: a unique document identifier"
            unless $_[0] eq '-doc_mdfive';
        $doc_mdfive = $_[1];
    }
    else {
        $doc_id = shift;
        $doc_mdfive = md5_hex($doc_id);
    }
    ### Iterate through the subkindexes.  
    my $deleted = undef;
    for my $knum (0 .. @{ $self->{subkindexes} }) {
        my $subkindex = $self->{subkindexes}[$knum];
        next unless exists $subkindex->{kinoids}{$doc_mdfive};
        my $doc_num = $subkindex->{kinoids}{$doc_mdfive};
    
        ### Retrieve stored info.
        ### TODO decode.
        $deleted = delete $subkindex->{kinodocs}{$doc_num};
        ### Add the document to a list of deleted documents.  
        ### It's been purged from kinodocs, but references to it are still
        ### peppered throughout the kinodata.  Those will go away when a
        ### kinodata rewrite is triggered - either by _consolidate_subkindex,
        ### or if the doc belongs to the outkindex, by generate().
        $self->{kinodel}{$doc_num}{$knum} = undef;
    }

    ### Keep track of the number of documents in the total collection.
    --$self->{num_docs} if defined $deleted;

    ### Return the stored fields for the document.
    return $deleted;
}

##############################################################################
### Determine whether a document has been indexed.
##############################################################################
sub doc_is_indexed {
    my $self = shift;
    my $doc_id = shift;
    my $doc_mdfive = md5_hex( $doc_id );
    for (@{ $self->{subkindexes} }) {
        return 1 if exists $_->{kinoids}{$doc_mdfive};
    }
}

##############################################################################
### Calculate the document's score for each term
##############################################################################
sub _score_terms {
    my ( $self, $doc, $doc_num, $to_score, $unstemmed ) = @_;
    
    my $sortstring = $doc->{sortstring};
    my $datetime_string = $doc->{datetime_string};
    
    my $stoplist = $self->{stoplist};
    my $norm_divisor = $self->{norm_divisor};
    my $scorable_fields = $self->{fields_score};
    my $field_defs = $self->{field_defs};
    
    my %aggscores;
    my %fieldscores;

    ### Iterate through *scorable* fields.
    foreach my $fieldname (@$scorable_fields) {
        my $scorable_terms = $to_score->{$fieldname};
        my %rawscores;
        ### Note: the tf/idf algo uses the number of tokens as a normalizing 
        ### factor.
        my $count_tokens = @$scorable_terms;
        next unless $count_tokens;
        {   
            no warnings 'uninitialized';
            if ($field_defs->{$fieldname}{-stem}) {
                my $stemless = $unstemmed->{$fieldname};
                for my $term (@$scorable_terms) {
                    next if exists $stoplist->{ shift @$stemless };
                    next if $term eq '';
                    $rawscores{$term} += 1;
                }
            }
            else {
                for my $term (@$scorable_terms) {
                    next if $term eq '';
                    $rawscores{$term} += 1;
                }
            }
        }
        ### Score token pairs if phrase matching is turned on.
        if ($self->{-phrase_matching}) {
            while ($#$scorable_terms) {
                my $pair = shift @$scorable_terms;
                $rawscores{"$pair $scorable_terms->[0]"} += 1;
            }
        }
        
        ### tf = term frequency in field
        ### norm = square root of total tokens in field
        
        my $fieldweight = $self->{fieldweights}{$fieldname};
        ### This will be multiplied against idf in the search app.
        foreach my $term (keys %rawscores) {
            $aggscores{$term} ||= 0;
            ### Divide the number of occurrences
            ### of one token by by the square root of the number of 
            ### tokens in the field.
            my $temp_score = $rawscores{$term} / (sqrt($count_tokens));
            ### Add the weighted score for the term in this field
            ### to the document's aggregate score for this term.
            $aggscores{$term} += $temp_score * $fieldweight;
            ### Prepare the field/term score for storage as a 16-bit 
            ### int. 
            $fieldscores{$fieldname}{$term} = $temp_score * 32_000;
        }
    }
    if (!defined $self->{kdt_pre_template}) {
        $self->{kdt_pre_template} = 'N n ' . ('n ' x @$scorable_fields); 
        ### 4 for the doc_num, 2 for the aggscore, 2 each for all scorable
        ### fields.
        $self->{kdt_pre_bytes} = 6 + 2 * @$scorable_fields;
    }

    my $kdt_pre_template = $self->{kdt_pre_template};

    ### Compose the precursor to the kinodata entry for each term in this doc.
    ### The term (or term pair), is separated from several fixed length items
    ### by a null byte.  The line separator terminates each entry.
    my @entries;
    foreach my $term (sort keys %aggscores) {
        my @fscores;
        for (@$scorable_fields) {
            my $val = exists $fieldscores{$_}{$term} ? 
                      $fieldscores{$_}{$term} : 0;
            push @fscores, $val;
        }
        
        ### Normalize the aggscore so that it will fit comfortably within a 
        ### 16-bit int.
        my $normalized_aggscore 
            = int(($aggscores{$term} / $norm_divisor) * 32_000);
            
        my $kinodata_entry = $term . "\0" . $sortstring . $datetime_string .
            pack($kdt_pre_template, $doc_num, $normalized_aggscore, @fscores);

        ### For now, we store the precursors in memory.  
        push @entries, $kinodata_entry;
    }
    $self->{kinodata_cache}->feed(@entries);
}

sub _consolidate_subkindex {
    my $self = shift;
    my $knum = shift;
    
    my $kinodata_only = 0;
    
    my $subkindexes = $self->{subkindexes};
    
    my $conkindex;
    if ($knum == $self->{outknum}) {
        $kinodata_only = 1;
        $conkindex = Search::Kinosearch::SubKindex->new(
            mpath => File::Spec->catdir($self->{mainpath}, "subkindex$knum"),
            fpath => File::Spec->catdir($self->{freqpath}, "freqdata$knum"),
            );
    }
    else {
        $conkindex = $subkindexes->[$knum];
    }
    
    ### Make a list of this subkindex's deletes.
    my $kinodel = $self->{kinodel};
    my %conkinodel;
    while (my ($dnum, $knum_hash) = each %$kinodel) {
        next unless exists $knum_hash->{$knum};
        $conkinodel{$dnum} = undef;
    }
    
    my $outkindex = $self->{outkindex};
    my $outkinoids = $outkindex->{kinoids};
    my $outkinodocs = $outkindex->{kinodocs};
    my $outkinoterms = $outkindex->{kinoterms};
    
    my $conkinoids = $conkindex->{kinoids};
    my $conkinodocs = $conkindex->{kinodocs};
    my $conkinoterms = $conkindex->{kinoterms};
    
    my %remapped_doc_nums;
    my $out_doc_num = $self->{doc_num};
    unless ($kinodata_only) {
        ### Transfer kinodocs, kinoprocs, kinoids
        while (my ($mdfive, $old_doc_num) = each %$conkinoids) {
            next if exists $conkinodel{$old_doc_num};
            $self->{num_docs}++;
            $out_doc_num++;
            $outkinoids->{$mdfive} = $out_doc_num;
            $remapped_doc_nums{$old_doc_num} = pack('N', $out_doc_num);
            $outkinodocs->{$out_doc_num} = $conkinodocs->{$old_doc_num};
        }
        $self->{doc_num} = $out_doc_num;
    }
    
    
    ### Add old kinodata to kinodata_cache.
    my $kdata_fields = $self->{fields_kdata};
    my $kdata_bytes = $self->{kdata_bytes};
    my %buffer_templates; 
    $buffer_templates{$_} = "a$kdata_bytes->{$_} " for @$kdata_fields;
    my $count_cache_lines = 0;
    my $count_precursor_lines = 0;
    my $sorted_terms = Sort::External->new;
    $sorted_terms->feed( "$_\n" ) for keys %$conkinoterms; ### loaded into mem? 
    $sorted_terms->finish;
    while (my $term = $sorted_terms->fetch) {
        chomp $term;
        my %kinoterms_entry;
        @kinoterms_entry{'doc_freq','offset','filenum'} =
            unpack('N N N', $conkinoterms->{$term});
        my $doc_freq = $kinoterms_entry{doc_freq};
        
        my @buffer = ("$term\0") x $doc_freq;
        
        my @invalid;
        for my $fieldname (@$kdata_fields) {
            my $packed_data;
            next unless $kdata_bytes->{$fieldname};
            $packed_data = $self->_read_kinodata(
                $conkindex, $fieldname, 
                @kinoterms_entry{'filenum','offset','doc_freq'});
            my @unpacked;
            if ($fieldname eq 'doc_num') {
                my @actual_numbers
                    = unpack(('N ' x $doc_freq), $packed_data);
                my $count = 0;
                if ($kinodata_only) {
                    for (@actual_numbers) {
                        if (exists $conkinodel{$_}) {
                            push @unpacked, '';
                            push @invalid, $count;
                        }
                        else {
                            push @unpacked, pack('N', $_);
                        }
                    }
                    $count++;
                }
                else {
                    for (@actual_numbers) {
                        if (exists $conkinodel{$_}) {
                            push @unpacked, '';
                            push @invalid, $count;
                        }
                        else {
                            push @unpacked, $remapped_doc_nums{$_};
                        }
                    $count++;
                    }
                }
            }
            else {
                @unpacked = unpack(
                    ($buffer_templates{$fieldname} x $doc_freq), 
                    $packed_data
                    );
            }
            $_ .= shift @unpacked for @buffer;
        }
        $buffer[$_] = '' for @invalid;
        $self->{kinodata_cache}->feed(@buffer);
    }
}
        

##############################################################################
### Generate kinodata, kinoterms, etc...
##############################################################################
sub generate {
    my $self = shift;
    my $subkindexes = $self->{subkindexes};
    my $optimization = $self->{optimization};
    my $kinodel = $self->{kinodel};
    my $outkindex = $self->{outkindex};
    my $outknum = $self->{outknum};

    $self->_consolidate_subkindex($_) for @{ $self->{to_consolidate} };

    my $scorable_fields = $self->{fields_score};

    my $heldover_term = '';
    my $record_filenum = 0;
    my $record_offset = 0;
    my $record_start = 0;
    my $record_length_in_lines = 0;

    my $kdata_filenum = 0;
    my @kdata_filehandles;
    my @kdata_fileparts = ('doc_num','aggscore', @$scorable_fields);
    my $doc_num_i = 0;
    my $test_kdata_length = 6 + 2 * @$scorable_fields;
    if ($self->{datetime_enabled}) {
        unshift @kdata_fileparts, 'datetime';
        $doc_num_i++;
        $test_kdata_length += 8;
    }
    if ($self->{sortstring_bytes}) {
        unshift @kdata_fileparts, 'sortstring';
        $doc_num_i++;
        $test_kdata_length += $self->{sortstring_bytes};
    }
    
    my $max_bytes_per_entry = 0;
    for (values %{ $self->{kdata_bytes} }) {
        $max_bytes_per_entry = $max_bytes_per_entry > $_ ?
                               $max_bytes_per_entry : $_;
    }
    $self->{entries_per_kdata_file}
        = int($self->{-max_kinodata_fs} / $max_bytes_per_entry);
    my $entries_per_kdata_file = $self->{entries_per_kdata_file};

    my $kdt_splitter_template = '';
    for (@kdata_fileparts) {
        $kdt_splitter_template .= "a$self->{kdata_bytes}{$_} ";
    }
        
    my $kinodata_cache = $self->{kinodata_cache};
    $kinodata_cache->finish;
    
    ### Iterate through the sorted list of kinodata precursors.
    ### Keep track of the location where one term begins and how many rows of
    ### kinodata it continues for.
    my $loopcount = 0;
    my $started = 0;
    my $heldover_line;

    while (defined (my $line = $kinodata_cache->fetch)) {
        next unless length $line;
        ++$loopcount;
        OPEN_KDATA: { # single iteration loop structure. 
            last unless $record_offset % $entries_per_kdata_file == 0;
            last unless (($loopcount == 1) or $record_offset);
            if ($loopcount and $record_offset) {
                close $_ for @kdata_filehandles;
                $kdata_filenum++; 
            }
            for my $filepart (@kdata_fileparts) {
                my $kinodata_filename = "$filepart$kdata_filenum.kdt";
                my $kinodata_filepath = File::Spec->catfile(
                    $outkindex->{fpath}, $kinodata_filename);
                $outkindex->{kinodata}{$filepart}{$kdata_filenum}{filepath} 
                    = $kinodata_filepath;
                my  $fh;
                sysopen ($fh, $kinodata_filepath, 
                   O_CREAT | O_WRONLY | O_EXCL) 
                        or die "Couldn't open file '$kinodata_filepath': $!";
                binmode $fh;
                push @kdata_filehandles, $fh;
                $outkindex->{kinodata}{$filepart}{$kdata_filenum}{handle} 
                    = $fh;
            }
            $record_offset = 0;
        }
        $line =~ s/^([^\0]*)\0// 
            or confess "Internal error. kinopre line content: $_";
        my $term = $1;
        my @kdata_values
            = unpack($kdt_splitter_template, $line);
        
        my $doc_num = unpack('N', $kdata_values[$doc_num_i]);
        ### Don't use this precursor line if it belongs to a deleted doc.
        next if exists $kinodel->{$doc_num}
            and exists $kinodel->{$doc_num}{$outknum};
        foreach my $fh (@kdata_filehandles) {
            print $fh shift @kdata_values;
        }
        
        ### If the term has changed between the last iteration and this
        ### one, we've reached the end of the kinodata.  Now that we know
        ### both where the kinodata related to this term starts and where
        ### it ends, record that lookup information in the kinoterms tied
        ### db hash.
        if ($heldover_term ne $term) {
            if ($started) {
                $outkindex->{kinoterms}{$heldover_term} = 
                    pack('N N N', 
                        $record_length_in_lines, # doc_freq
                        $record_start,           # offset
                        $record_filenum,         # filenum
                        );
            }
            ### Prepare for the next term.
            $record_length_in_lines = 0;
            $record_start = $record_offset;
            $record_filenum = $kdata_filenum;
        }

        $heldover_term = $term;
        $record_offset++;
        $record_length_in_lines++;

        $started ||= 1;
    }
    close $_ for @kdata_filehandles;
    ### Record the last remaining entry
    $outkindex->{kinoterms}{$heldover_term} = 
        pack('N N N', 
            $record_length_in_lines, # doc_freq
            $record_start,           # offset
            $record_filenum,         # filenum
            );

    ### We're finished modifying the kindex data, so record its stats. 
    $self->_write_kinostats;

    ### Now that deletes are actually purged from the kindex, modify the
    ### kinodel list.
    while (my ($doc_num, $knumhash) = each %$kinodel) {
        for (keys %$knumhash) {
            next unless $_ >= $outknum;
            delete $knumhash->{$_};
        }
        delete $kinodel->{$doc_num} unless %$knumhash;
    }
    
    ### Store deleted docs in a file.
    my $kinodel_filepath = File::Spec->catfile($self->{temp_dir}, 'kinodel');
    nstore($kinodel, $kinodel_filepath);

    ### Deactivate all the subkindexes;
    for my $subkindex (@{ $self->{subkindexes} }) {
        untie $_ for values %$subkindex;
    }
}

sub write_kindex {
    my $self = shift;

    my $subkindexes = $self->{subkindexes};
    my $optimization = $self->{optimization};
    
    ### Now that the kindex files are ready to write out, establish an
    ### exclusive lock on the entire kindex.
    my $readlockfh = $self->{kinoreadlock}{fh};
    print "Waiting for exclusive lock on $self->{kinoreadlock}{path}..." if
        $self->{-verbosity};
    flock($readlockfh, LOCK_UN)
        or croak("Couldn't unlock '$self->{kinoreadlock}{path}': $!");
    flock($readlockfh, LOCK_EX)
        or croak("Couldn't get lock on '$self->{kinoreadlock}{path}': $!");
    print " got it\n" if $self->{-verbosity};

    ### Blow away unneeded subkindexes
    my @to_destroy;
    if ($self->{-mode} eq 'update') {
        @to_destroy = @{ $self->{to_consolidate} };
    }
    else  {
        opendir MAINPATH, $self->{mainpath};
        my @subkindexes = grep { /^subkindex\d+$/ } readdir MAINPATH;
        closedir MAINPATH;
        @to_destroy = map { /^subkindex(\d+)$/; $1 } @subkindexes;
    }
    for (@to_destroy) {
        my $mpath = File::Spec->catdir($self->{mainpath}, "subkindex$_");
        my $fpath = File::Spec->catdir($self->{freqpath}, "freqdata$_");
        $self->_purge_subkindex($mpath, $fpath);
    }
         
    my ($mpath, $fpath) = @{ $self->{outkindex} }{'mpath','fpath'};
    my $outknum = $self->{outknum};
    
    ### TODO Consider security implications.  Is there a way to make
    ### absolutely sure move() doesn't clobber anything?
    my $mpath_dest = File::Spec->catdir(
        $self->{mainpath}, "subkindex$outknum");
    my $fpath_dest = File::Spec->catdir(
        $self->{freqpath}, "freqdata$outknum");
    move($mpath, $mpath_dest) 
        or croak ("Couldn't move directory '$mpath' to '$mpath_dest': $!");
    move($fpath, $fpath_dest) 
        or croak ("Couldn't move directory '$fpath' to '$fpath_dest': $!");
        
    for (qw( kinodel kinostats ) ) {
        my $temppath = File::Spec->catfile( $self->{temp_dir}, $_);
        my $destpath = File::Spec->catfile( $self->{mainpath}, $_);
        if (-e $destpath) {
            unlink $destpath or croak("Couldn't unlink '$destpath': $!");
        }
        move($temppath, $destpath) 
            if -e $temppath;
    }
    
    flock($readlockfh, LOCK_UN)
        or croak("Couldn't unlock file '$self->{kinoreadlock}{path}': $!");
}

##############################################################################
### Manually clean out subkindex files.
### (rmtree would be easier, but this is safer.)
##############################################################################
sub _purge_subkindex {
    my $self = shift;
    my $mpath = shift;
    my $fpath = shift;

    for (qw( kinodocs kinoids )) {
        my $path = File::Spec->catfile($mpath, $_);
        next unless -e $path;
        unlink $path or  die "Couldn't unlink file '$path': $!";
    }
    opendir FPATH, $fpath
        or confess("Couldn't read directory '$fpath': $!");
    my @files = grep { /^[a-zA-Z_]+\d+\.kdt$/ } readdir FPATH;
    closedir FPATH;
    push @files, 'kinoterms';
    for (@files) {
        $_ = File::Spec->catfile($fpath, $_);
        unlink $_ or die "Couldn't unlink file '$_': $!";
    }
    rmdir $mpath or croak("Couldn't rmdir '$mpath': $!");
    rmdir $fpath or croak("Couldn't rmdir '$fpath': $!");
}

##############################################################################
### Record the kindex's vital statistics 
##############################################################################
sub _write_kinostats {
    my $self = shift;

    my %stats;
    my @members= qw(  field_defs
                      num_docs 
                      doc_num
                      language
                      encoding 
                      version 
                      entries_per_kdata_file
                      datetime_enabled
                      sortstring_bytes
                      );
        @stats{@members} = @{ $self }{@members};
    my $path = File::Spec->catfile($self->{temp_dir}, 'kinostats');
    nstore \%stats, $path;
}

1;


__END__

=head1 NAME

Search::Kinosearch::Kindexer - create kindex files

=head1 WARNING

Kinosearch is ALPHA test software. 

Please read the full warning in the L<Search::Kinosearch|Search::Kinosearch>
documentation.

=head1 SYNOPSIS

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

=head1 DESCRIPTION

=head2 Create a kindex

How to create a kindex, in 7 easy steps...

Step 1: Create a Kindexer object.

    my $kindexer = Search::Kinosearch::Kindexer->new(
        -mainpath       => '/foo/bar/kindex',
        -temp_directory => '/foo/bar',
        -mode           => 'overwrite',
        );

Step 2: Define all the fields that you'll ever need this kindex to have --
because as soon as you process your first document, you lose the ability to
add, remove, or change the characteristics of any fields.  

    $kindexer->define_field(
        -name   =>  'url',
        -score  =>  0,
        );
    $kindexer->define_field(
        -name   => 'title',
        -lowercase => 1,
        -tokenize  => 1,
        -stem      => 1,
        );
    $kindexer->define_field(
        -name      => 'bodytext',
        -lowercase => 1,
        -tokenize  => 1,
        -stem      => 1,
        );
    $kindexer->define_field(
        -name   => 'keywords',
        -lowercase => 1,
        -tokenize  => 1,
        -stem      => 1,
        -store  => 0,
        );
    $kindexer->define_field(
        -name   => 'section',
        -weight => 0,
        );

Step 3: Start a new document, identified by something unique (such as a URL).

    my $doc = $kindexer->new_doc($url);

Step 4: set the value for each field.

    $doc->set_field( url      => $url      );
    $doc->set_field( title    => $title    );
    $doc->set_field( bodytext => $bodytext );
    $doc->set_field( keywords => $keywords );
    $doc->set_field( section  => $section  );

Step 5: Add the document to the kindex;

    $kindexer->add_doc( $doc ); 

Step 6: Repeat steps 3-5 for each document in the collection.

Step 7: Finalize the kindex and write it out. 

    $kindexer->generate;
    $kindexer->write_kindex;

=head2 Update an existing kindex

Other than making sure that -mode is set to 'update', there is no difference
in how you treat the Kindexer, though you may wish to choose a custom setting for
-optimization.

    my $kindexer = Search::Kinosearch::Kindexer->new(
        -mainpath       => '/path/to/kindex',
        -temp_directory => '/foo/bar',
        -mode           => 'update',
        -optimization   => 1,
        );

If you want to overwrite a document currently in the kindex, simply call
new_doc() etc, the way you did when you added it to the kindex in the
first place, making sure that the unique identifier matches.

=head2 File Locking

Whenever you create a Kindexer, a KSearch or a Kindex object, a shared lock
is requested on a file called 'kinoreadlock' within the -mainpath directory.
Kindexer objects perform all of their file manipulation on temporary files
which are swapped in at the last moment, so it is safe to continue searching
against an existing kindex while it is in the process of being updated, or 
even overwritten.  

After the Kindexer recieves the shared lock on 'kinoreadlock', it requests an
exclusive lock on another file called 'kinowritelock'.  If it cannot get this 
exclusive lock, it bombs out immediately, since it is not safe for two 
Kindexers to run updates against the same kindex simultaneously.

After $kindexer->generate() completes, the files are ready to be swapped into
place using L<File::Copy|File::Copy>'s move() command.  Calling write_kindex()
triggers a request for an exclusive lock on 'kinoreadlock', for which the
Kindexer will wait as long as necessary.  Once the exclusive lock is granted,
the outdated files are unlinked, the new files take their spots, and all locks
are released.  If the the temp directory and the kindex are on the same
volume, the process can be almost instantaneous.  

=head1 METHODS

=head2 new()

    my $kindexer = Search::Kinosearch::Kindexer->new(
        -mode            => 'overwrite',       # default: 'update'
        -mainpath        => '/foo/bar/kindex', # default: ./kindex
        -freqpath        => '/baz/freqdata',   # default: ./kindex/freqdata
        -temp_directory  => '/foo/temp'        # default: current directory
        -optimization    => 1,                 # default: 2
        -language        => 'Es',              # default: 'En'
        -encoding        => 'UTF-8',           # default: 'UTF-8'
        -phrase_matching => 0,                 # default: 1
        -enable_datetime => 1,                 # default: 0
        -stoplist        => \%stoplist,        # default: see below
        -max_kinodata_fs => 2 ** 29,           # default: 2 ** 28 [256 Mb]
        -verbosity       => 1,                 # default: 0
        );

Create a Kindexer object.

=over

=item -mode

Two options are available: 'overwrite' and 'update'.  If there is no kindex at
the specified -mainpath, a kindex will be created no matter what -mode is set
to.  In either case, no permanent file modifications beyond the creation of
-mainpath, -freqpath, and the lockfiles are applied until write_kindex() is
called.

=item -mainpath

The path to your kindex.  If not specified, defaults to 'kindex'.

=item -freqpath

Files within this directory contain term frequency data.  The speed with which
they can be read has a major impact on search-time performance, so you may
wish to copy this directory onto a ram disk once Kindexer finishes.  If you
don't specify -freqpath, it appears as a directory called 'freqdata' within
-mainpath.

=item -temp_directory

The Kindexer object will create a single randomly-named temp directory within
whatever is specified as -temp_directory, then use that inner directory for
all its temporary files. B<BUG:> In the 0.02 branch of Kinosearch, the
-temp_directory MUST be on the same filesystem as the kindex itself. 

=item -optimization

This parameter, which controls the behavior of update mode, is primarily
relevant for large-scale Kinosearch deployments that require frequent updates.
For small scale deployments, the simplest course is to run Kindexer with -mode
set to 'overwrite' and regenerate the kindex from scratch every time -- in
which case -optimization is irrelevant.  

There are 4 possible settings for -optimization: 

1 - Full optimization. Long indexing times, but quick searches.  All
subkindexes are merged into one every time.

2 - Close to full optimization (default setting). Searches perform at close to
maximum speed; index times are usually pretty short, but every once in a while
a spike occurs.  A maximum of 2 subkindexes are allowed to exist at any given
moment.  If the second (auxilliary) subkindex is detected to be larger than
10% of the size of the first (primary) subkindex when the Kindexer starts,
-optimization is kicked up to level 1 and the two are merged. 

3 - The "incremental indexing" setting.  Indexing times are usually quick,
though spikes occur; search times may be somewhat slower, though the
difference is minimal if you are using a ram disk.  Subkindexes are
consolidated either when there are 10 of them, or when several of them contain
as many documents as their left neighbor.  The goal is to minimize the
resources expended on consolidating subkindexes while maintaining decent
search-time performance.

4 - No optimization.  Short indexing times; search performance degrades the
more the kindex is updated, and doesn't recover until it is updated with
-optimization set to 1, 2, or 3.  Subkindexes are not merged -- a new
subkindex is tacked on to the end of the kindex every time Kindexer is called
upon to update it.

=item -language

The language of the documents being indexed.  Options: the final part of any
Search::Kinosearch::Lingua::Xx module name e.g. 'Es' (Spanish), 'Hr'
(Croatian). [At present, only 'En' works.] This setting determines the
algorithms used for stemming and tokenizing.  See the
L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua> documentation for
details.

=item -encoding

This doesn't do anything yet.

=item -phrase_matching

If set to 1, word pairs will be indexed along with individual words.  Enabling
phrase matching at index-time is required for enabling phrase matching at
search-time, because if the word pairs aren't in there, the phrase matching
algorithm breaks.  Disabling phrase matching reduces the size of the kindex
considerably.  See the L<Search::Kinosearch|Search::Kinosearch> documentation
for a discussion of Kinosearch's phrase-matching algorithm.

=item -enable_datetime

Set this to 1 if you want to be able to assign datetimes to individual records
(see L<Search::Kinosearch::Doc|Search::Kinosearch::Doc>) and sort searches by
datetime.  Note that enabling datetime increases the size of your kindex,
specifically the frequency data portion.

=item -stoplist

The default stoplist for each language is defined in its Lingua module (e.g.
L<Search::Kinosearch::Lingua::En|Search::Kinosearch::Lingua::En>). If you wish
to use a custom stoplist, supply a hashref pointing to a hash where the keys
are all stopwords.

=item -max_kinodata_fs

Set the maximum size for a kinodata file, in bytes.  At present, changing this
won't do anything significant.

=item -verbosity

Verbose (debugging) output.  At present, this only tells you the progress of
flock calls.

=back

=head2 define_field()

Kinosearch conceptualizes each document like a row in a database table: as a
collection of discrete fields.  Before you add any documents to the kindex,
you must define attributes for each field.

    $kindexer->define_field(
        -name            => 'category', # required (no default)
        -store           => 1,          # default: 1
        -score           => 1,          # default: 1
        -weight          => 2,          # default: 1
        -lowercase       => 1,          # default: 0
        -tokenize        => 1,          # default: 0
        -stem            => 1,          # default: 0
        );

=over

=item -name

The name of the field.  Can contain only [a-zA-Z_].

=item -store

If -store is set to 1 (the default), the field's contents will be recorded in
the kindex and available for retrieval at search-time.  Examples: title, text,
URL, and so forth would typically have -store set to 1, so that their contents
could be used in the presentation of search results; keywords fields would
most often have -store set to 0. 

=item -score

If -score is set to 1 (the default), the field's contents will be included in
the kindex and considered by default when determining the score of a document
against a given search phrase.  Note that it is possible to issue
field-specific queries at search-time, so the -score attribute is not the only
tool for determining which fields are to be considered.  Example: URL fields
would typically have -score set to 0.

=item -weight

If weight is set to a number other than 1, then this field will contribute
more heavily to a document's aggregate score for any term within it than would
otherwise be the case.  You can also weight a field more heavily at
search-time, but since KSearch is slightly more efficient if it can use the
aggregate score, apply field-weighting at index-time if you can.  Note that
if you perform field-weighting at search-time, any weighting that you set with
define_field at index-time will be ignored.

=item -lowercase

Lowercase the text to be indexed.  (The copy of text to be stored will be not
be affected.)  

=item -tokenize

Tokenize the text to be indexed.  Not all fields should be tokenized -- for
example, there is rarely any point in tokenizing a URL.  

=item -stem

Stem the text to be indexed.

=back

=head2 new_doc()

    my $doc = $kindexer->new_doc( $doc_id );

Spawn a L<Search::Kinosearch::Doc|Search::Kinosearch::Doc> object.

One argument is required: a unique identifier.  The identifier could be a
database primary key, a URL, a filepath, or anything else.  If the document's
contents change later and you wish to update the kindex to reflect that
change, use the same identifier.

=head2 add_doc()

    $kindexer->add_doc( $doc );

Add a document, in the form of a Search::Kinosearch::Doc object, to the
kindex.

=head2 delete_doc()

    $kindexer->delete_doc( $doc_id );

Delete a document from the kindex.

=head2 doc_is_indexed()

    my $confirmation = $kindexer->doc_is_indexed( $doc_id );

Check for the existence of a document in the kindex.

=head2 generate()

    $kindexer->generate();

Complete the kindex, but don't save it just yet.  Note: depending on how many
files have been indexed this pass and how much optimation has to take place,
generate() can take a while.

=head2 write_kindex()

    $kindexer->write_kindex(); 

Clear out existing kindex files as necessary -- all of them in overwrite mode,
some of them in update mode -- and use File::Copy's move() command to transfer
new files to their destinations.

=begin comment

=head2 init_kindexer()

Private.

=end comment

=head1 BUGS

The -temp_directory must be on the same filesystem as -mainpath, or the move()
operation may fail.

=head1 TO DO

=over

=item

Add more verbose output.

=back

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch|Search::Kinosearch>

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=item

L<Search::Kinosearch::Kindex|Search::Kinosearch::Kindex>

=item

L<Search::Kinosearch::Tutorial|Search::Kinosearch::Tutorial>

=item

L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua>

=back

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut
