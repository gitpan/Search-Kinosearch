package Search::Kinosearch::Kindex;
use strict;
use warnings;

use Carp;
use File::Spec;
use Storable qw( nfreeze thaw retrieve );
use Fcntl qw(:DEFAULT :flock);

##############################################################################
### Create a Search::Kinosearch::Kindex object
##############################################################################
sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->init_kindex(@_);
    return $self;
}

my %init_kindex_defaults = (
    -mainpath         => undef,
    -freqpath         => undef,
    -mode             => 'readonly',
    version           => undef,
    );

##############################################################################
### Initialize the kindex.
##############################################################################
sub init_kindex {
    my $self = shift;
    
    ### Verify and assign parameter values
    %$self = (%init_kindex_defaults, %$self);
    while (@_) {
        my ($var, $val) = (shift,shift);
        croak("Invalid parameter: '$var'")
            unless exists $init_kindex_defaults{$var};
        $self->{$var} = $val;
    }
    croak("Invalid value for parameter mode: '$self->{-mode}'")
        if $self->{-mode} !~ /^(?:readonly|overwrite|update)$/;
        
    $self->{$_} = []
        for (qw( subkindexes fields_kdata fields_all fields_score fields_store 
                 fields_lowercase fields_tokenize fields_stem ));
    $self->{kdata_bytes}     = {
        doc_num => 4,
        aggscore => 2,
        sortstring => 0,
        datetime => 0,
        };
    $self->{kinodel} = {};
    
    ### Clean up supplied pathnames.
    $self->{mainpath} = defined $self->{-mainpath} ?
            $self->{-mainpath} :
            File::Spec->catdir( File::Spec->curdir, 'kindex');
    $self->{mainpath} = File::Spec->rel2abs($self->{mainpath});
    $self->{freqpath} = defined $self->{-freqpath} ?
        $self->{-freqpath} :
        File::Spec->catdir($self->{mainpath},'freqdata');
    $self->{freqpath} = File::Spec->rel2abs($self->{freqpath});

    ### Create the primary filepath and the freqdata filepath, if they
    ### don't already exist and we're in update/overwrite mode.
    if ($self->{-mode} =~ /^(?:overwrite|update)$/) {
        for ($self->{mainpath}, $self->{freqpath}) {
            next if -d $_;
            mkdir $_ or croak("Couldn't create directory '$_': $!");
        }
    }
    
    ### Establish a shared read lock on the entire kindex.
    my $lfpath = $self->{kinoreadlock}{path} = File::Spec->catfile(
        $self->{mainpath}, 'kinoreadlock');
    my $lockfh;
    my $readlockflags = -e $lfpath ? 
                        O_RDONLY : (O_CREAT | O_EXCL | O_WRONLY);
    sysopen $lockfh, $lfpath, $readlockflags
        or croak "Couldn't open lockfile '$lfpath': $!";
    print STDERR "waiting for lock..." if $self->{-verbosity};
    flock($lockfh, LOCK_SH)
        or croak ("Couldn't establish shared lock " .
                  "on lockfile '$lfpath': $!");
    print STDERR "got it.\n" if $self->{-verbosity};
    $self->{kinoreadlock}{fh} = $lockfh;

    ### Load an existing kindex 
    my $stats_filepath = File::Spec->catfile($self->{mainpath}, 'kinostats');
    if ($self->{-mode} =~ /^(?:readonly|update)$/ and -e $stats_filepath) {
        ### Get essential info about the kindex from the kinostats file.
        my $kinostats = retrieve($stats_filepath);
        my ($version) = $Search::Kinosearch::VERSION =~ /([^_]+)_?/;
        carp("Kinosearch version '$version' doesn't " .
            "match version for this kindex: '$kinostats->{version}")
                if $version ne $kinostats->{version};
        
        ### Load deleted file numbers into memory.
        my $kinodel_path = File::Spec->catfile($self->{mainpath}, 'kinodel');
        $self->{kinodel} = retrieve($kinodel_path) if -e $kinodel_path;
        
        @{ $self }{ keys %$kinostats } = values %$kinostats;
        $self->{kdata_bytes}{sortstring} = $self->{sortstring_bytes};
        $self->{kdata_bytes}{datetime} = $self->{datetime_enabled} ? 8 : 0;
        
        my $field_defs = $self->{field_defs};
         $self->define_field(%{ $field_defs->{$_} }) for keys %$field_defs;

        ### Open existing subkindexes    
        opendir MAINPATH, $self->{mainpath}
            or croak("Couldn't open directory '$self->{mainpath}' :$!");
        my @subkindexes = grep { /^subkindex\d+$/ } readdir MAINPATH;
        croak("Couldn't find any existing subkindexes in $self->{mainpath}")
            unless @subkindexes;
        my @kindexnums = map { /(\d+)/; $1 } @subkindexes;
        @kindexnums = sort { $a <=> $b } @kindexnums;
        closedir MAINPATH;

        for my $kindexnum (@kindexnums) {
            my $mpath = File::Spec->catdir(
                $self->{mainpath}, "subkindex$kindexnum");
            my $fpath = File::Spec->catdir(
                $self->{freqpath}, "freqdata$kindexnum");
            $self->{subkindexes}[$kindexnum]
                = Search::Kinosearch::SubKindex->new(
                    mode      => 'readonly',
                    mpath     => $mpath,
                    fpath     => $fpath,
                    );
        }
    }
}

my %define_field_defaults = (
    -name                   => undef, 
    -store                  => 1,
    -score                  => 1,
    -weight                 => 1,
    -tokenize               => 0,
    -stem                   => 0,
    -lowercase              => 0,
    -kdata_bytes            => 2,
);

##############################################################################
### Define the attributes of a kindex field.
##############################################################################
sub define_field {
    my $self = shift;
    croak ("Can't define any fields once documents have been added")
        if (defined $self->{initialized});
    
    ### Verify and assign parameters.  Make sure that there are no field name
    ### conflicts, either with reserved names, or with names of fields which
    ### were previously defined.
    my %params = %define_field_defaults;
    while (@_) {
        my ($var, $val) = (shift, shift); 
        croak ("Invalid parameter: '$var'") 
            unless exists $define_field_defaults{$var};
        $params{$var} = $val; 
    }
    croak("Attribute -name must be composed of letters and underscores") 
        unless $params{-name} =~ /^[a-zA-Z_]+$/;
    my $fieldname = $params{-name};
    if (exists $self->{fields}{$fieldname}
        or $fieldname =~ /^(?:score|excerpt|aggscore|datetime|sortstring)$/) 
    {
        croak("The name '$fieldname' is already reserved. Please "
            . "choose a different one.");
    }
    
    ### Store the field specs, and establish the buffer used by set_field
    $self->{fields}{$fieldname} = {};
    $self->{field_defs}{$fieldname} = \%params;
    
    ### Sort fieldnames into arrays which can be iterated through efficiently.
    for (qw( all tokenize score store stem lowercase )) {
        next unless $_ eq 'all' or $params{"-$_"};
        @{ $self->{"fields_$_"} } 
            = sort @{ $self->{"fields_$_"} }, $fieldname;
    }
    if ($params{-score}) {
        @{ $self->{fields_kdata} } = ('sortstring', 'datetime', 'doc_num',
            'aggscore', @{ $self->{fields_score} });
        $self->{kdata_bytes}{$fieldname} = $params{-kdata_bytes};
    }
    $self->{fieldweights}{$fieldname} = $params{-weight};
    $self->{norm_divisor} = 0;
    $self->{norm_divisor} += $self->{fieldweights}{$_}
        for @{ $self->{fields_score} };
}

##############################################################################
### Retrieve the relevant section of kinodata, from multiple files 
### if necessary. 
##############################################################################
sub _read_kinodata {
    my ($self, $kindex, $scorefield, 
        $scorefile_num, $start, $lines_to_grab) = @_;
    my $entries_per_kdata_file = $self->{entries_per_kdata_file};
    my $bytes_per_entry = $self->{kdata_bytes}{$scorefield};
    return '' unless $bytes_per_entry;
    my $packed_data = '';
    while ($lines_to_grab) {
        ### If all the (remaining) data is in one .kdt file, and doesn't cross 
        ### boundaries...
        if (($start % $entries_per_kdata_file + ($lines_to_grab)) 
                < $entries_per_kdata_file) 
        {       
            my $relative_start = $start % $entries_per_kdata_file;
            my $fh = $kindex->{kinodata}{$scorefield}{$scorefile_num}{handle};
            seek $fh, ( $relative_start * $bytes_per_entry ), 0;
            my $packed_data_temp;
            read ($fh, $packed_data_temp, 
                ( $lines_to_grab * $bytes_per_entry ));
            $packed_data .= $packed_data_temp;
            last;   
        }       
        ### If the data is spread over multiple files...
        else {  
            my $relative_start = $start % $entries_per_kdata_file;
            my $num_to_read = ($entries_per_kdata_file 
                - ($start % $entries_per_kdata_file));
        
            last if !exists $kindex->{kinodata}{$scorefield}{$scorefile_num};
            my $fh = $kindex->{kinodata}{$scorefield}{$scorefile_num}{handle};
            seek $fh, ( $relative_start * $bytes_per_entry ), 0;
            my $packed_data_temp;
            read ($fh, $packed_data_temp, 
                ( $num_to_read * $bytes_per_entry ));
            $packed_data .= $packed_data_temp;

            $scorefile_num++;
            $start = 0;
            $lines_to_grab -= $num_to_read;
        }       
    }
    return $packed_data;
}

package Search::Kinosearch::SubKindex;

use Carp;
use File::Spec;
use Storable qw( nfreeze thaw retrieve );
use DB_File;
use Fcntl qw(:DEFAULT :flock);

sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->init_subkindex(@_);
    return $self;
}

my %init_subkindex_defaults = (
    mode                   => 'readonly',
    mpath                  => undef,
    fpath                  => undef,
    );

##############################################################################
### Initialize a subkindex.
##############################################################################
sub init_subkindex {
    my $self = shift;
    
    ### Verify and assign parameter values
    %$self = (%init_subkindex_defaults, %$self);
    while (@_) {
        my ($var,$val) = (shift, shift);
        croak("Invalid parameter: '$var'")
            unless exists $init_subkindex_defaults{$var};
        $self->{$var} = $val;
    }
    croak("Invalid value for parameter mode: '$self->{mode}'")
        unless $self->{mode} =~ /^(?:create|update|readonly)$/;

    if ($self->{mode} =~ /^(?:readonly|update)$/) {
        ### Initialize the kinodata files.
        opendir FPATH, $self->{fpath} 
            or croak("Couldn't open directory '$self->{fpath}': $!");
        my @files = grep { /\d+\.kdt/ } readdir FPATH;
        closedir FPATH;
        for (@files) {
            my ($filepart,$num) = /([a-zA-Z_]+)(\d+)\.kdt$/;
            my $filepath = File::Spec->catfile(
                $self->{fpath}, $_);
            my $fh;
            sysopen($fh, $filepath, O_RDONLY)
                or croak("Couldn't open file '$filepath': $!");
            flock($fh, LOCK_SH)
                or croak("Couldn't lock file '$filepath': $!");
            binmode $fh;
            $self->{kinodata}{$filepart}{$num}{filepath} = $filepath;
            $self->{kinodata}{$filepart}{$num}{handle} = $fh;
        }
        
    }
    
    ### Initialize all the tied hash dbs. 
    my $flags = $self->{mode} eq 'create' ?  
                (O_CREAT | O_RDWR | O_EXCL) : 
                $self->{mode} eq 'update' ?   
                (O_RDWR) : (O_RDONLY);
#    for my $component (qw( kinodocs kinoids kinoterms )) {
#        my $path = $component eq 'kinoterms' ?
#                   File::Spec->catfile($self->{fpath}, $component) : 
#                   File::Spec->catfile($self->{mpath}, $component);
#        $self->{$component} = {};
#        tie %{ $self->{$component} }, 'DB_File', $path, $flags
#                or croak("Can't init tied db hash '$path': $!");
#    }
     tie %{ $self->{kinodocs} }, 'DB_File',
         File::Spec->catfile($self->{mpath}, 'kinodocs'), 
         $flags; 
     tie %{ $self->{kinoids} }, 'DB_File',
         File::Spec->catfile($self->{mpath}, 'kinoids'), 
         $flags; 
     tie %{ $self->{kinoterms} }, 'DB_File',
         File::Spec->catfile($self->{fpath}, 'kinoterms'), 
         $flags; 
}

1;

__END__

=head1 NAME

Search::Kinosearch::Kindex - A Kinosearch index

=head1 SYNOPSIS

    my $kindex = Search::Kinosearch::Kindex->new();

=head1 DESCRIPTION

A Kindex object is the logical representation of a kindex created by
Search::Kinosearch::Kindexer.  

=head1 METHODS

=head2 new()

    my $kindex = Search::Kinosearch::Kindex->new(
        -mainpath      => '/foo/bar/kindex', # default: ./kindex
        -freqpath      => '/baz/freqdata'    # default: ./kindex/freqdata 
        );

Construct a Kindex object.   

Kindex objects on their own are useless, so there is only one reason that you
would ever do this: to create a persistent Kindex object under mod_perl that
you can feed to multiple KSearch objects.  Loading all your Kinosearch
modules *and* creating a Kindex object (which involves opening a bunch of
files, tying hashes, etc) when Apache starts cuts down somewhat on search-time
overhead.

Be advised that creating a persistent Kindex object establishes a read lock on
the kindex (specifically, on a file called 'kinoreadlock').  You can
start a Kindexer process while this read lock is in place, but it must be
released before you write_kindex().

=over

=item -mainpath

The location of your kindex.

=item -freqpath

The directory containing certain files which are crucial to search time
performance.  By default, this is a directory called 'freqdata' within
whatever you specified as -mainpath.

=back

=begin comment

=head2 init_kindex()

=head2 define_field()

Although define_field() is located within Search::Kinosearch::Kindex, it's only
available publicly through a Kindexer.

=end comment

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch|Search::Kinosearch>

=item

L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer>

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=item

L<Search::Kinosearch::Tutorial|Search::Kinosearch::Tutorial>

=item

L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua>

=back

=head1 AUTHOR

Marvin Humphrey <marvin at rectangular dot com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut



