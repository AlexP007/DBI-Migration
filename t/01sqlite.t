use 5.24.0;

use strict;
use warnings;

use DBI;
use DBI::Migrations;
use Data::Dumper;
use Test::SQLite;

use Test::Simple tests => 1;

# Use an in-memory test db:
my $sqlite = Test::SQLite->new(
    memory => 1, 
    db_attrs => { RaiseError => 1, AutoCommit => 1 },
);

my $dbh = $sqlite->dbh;

my $migrations = DBI::Migrations->new({
    dbh  => $dbh,
    dir  => 'db/migrations',
    name => 'test.db',
});

# TEST 1
# Testing applied_migration table creation
$migrations->init();

my $sth = $dbh->table_info('%', '%', 'applied_migrations', 'TABLE');
my @row = $sth->fetchrow_array;

ok(@row, 'Table applied_migrations does not exists');

$dbh->disconnect;