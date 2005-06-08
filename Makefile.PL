use 5.006001;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Search::Kinosearch',
    VERSION_FROM      => 'lib/Search/Kinosearch.pm', # finds $VERSION
    PREREQ_PM         => {
        'Digest::MD5'                   => 2.33,
        'Compress::Zlib'                => 1.33,
        'Lingua::Stem::Snowball'        => 0.7,
        'Test::Exception'               => 0.15,
        'Sort::External'                => 0.02,
        'String::CRC32'                 => 1.3,
        },
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Search/Kinosearch.pm', # retrieve abstract from module
       AUTHOR         => 'Marvin Humphrey <marvin at rectangular.com>') : ()),

);
