#!perl -T

use strict;
use warnings;
use Test::DBO SQLite => 6;

# Run in a temporary directory
mkdir 'tmp';
chdir 'tmp' or die $!;
END { rmdir '../tmp' or die $! }

# Create the DBO
my $dbo = Test::DBO::connect_ok;

my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_tbl = $dbo->_qi($test_tbl);

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, $quoted_tbl);

