use strict;
use warnings;
use Test::More;

# Ensure a recent version of Test::Pod::Coverage
my $min_tpc = 1.08;
eval "use Test::Pod::Coverage $min_tpc";
plan skip_all => "Test::Pod::Coverage $min_tpc required for testing POD coverage" if $@;

# Test::Pod::Coverage doesn't require a minimum Pod::Coverage version,
# but older versions don't recognize some common documentation styles
my $min_pc = 0.18;
eval "use Pod::Coverage $min_pc";
plan skip_all => "Pod::Coverage $min_pc required for testing POD coverage" if $@;

# Similar to Pod::Coverage::CountParents, but doesn't compile modules to find parents,
# parents to check are provided in %isa below
{
    $INC{'Pod/Coverage/DBO.pm'} = 1;
    package Pod::Coverage::DBO;
    our @ISA = ('Pod::Coverage');
    my %isa = (
        'DBIx::DBO' => [qw(DBIx::DBO::Common)],
        'DBIx::DBO::Handle' => [qw(DBIx::DBO DBIx::DBO::Common)],
        'DBIx::DBO::Table' => [qw(DBIx::DBO::Common)],
        'DBIx::DBO::Query' => [qw(DBIx::DBO::Common)],
        'DBIx::DBO::Row' => [qw(DBIx::DBO::Common)],
        'DBIx::DBO::DBD::DBM' => [qw(DBIx::DBO)],
        'DBIx::DBO::DBD::SQLite' => [qw(DBIx::DBO)],
        'DBIx::DBO::DBD::mysql' => [qw(DBIx::DBO::Query)],
        'DBIx::DBO::DBD::Pg' => [qw(DBIx::DBO)],
    );
    my %pods;
    sub _get_pods {
        my $self = shift;
        unless (exists $pods{$self->{package}}) {
            my @pods = map { defined $_ ? (@$_) : () } $self->SUPER::_get_pods,
                map Pod::Coverage::DBO->new(package => $_)->_get_pods, @{$isa{$self->{package}}};
            $pods{$self->{package}} = \@pods if @pods;
        }
        return $pods{$self->{package}};
    }
}

all_pod_coverage_ok({ coverage_class => 'Pod::Coverage::DBO' });
