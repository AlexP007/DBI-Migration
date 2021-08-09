package DBI::Simple::Migration;

use 5.24.0;

use strict;
use warnings;
use feature 'say';

use Exporter 'import';

use Moo;
use Term::ANSIColor;
use File::Slurper 'read_text';
use Scalar::Util 'blessed';

use constant {
    UP   => 'up',
    DOWN => 'down',
};

our $VERSION = '1.00';

has dbh => (
    is       => 'ro',
    requires => 1,
    isa      => sub {
        die red("$_[0] is not DBI::db") unless blessed $_[0] and $_[0]->isa('DBI::db');
    },
);

has [qw(dir name)] => (
    is       => 'ro',
    required => 1,
);

sub init {
    my ($self) = @_;

    my @sql = <DATA>;
    my $sql = join '', @sql;

    if ($self->_is_applied_migrations_table_exists() ) {
        say yellow("Table applied_migrations already exists");
        return 1;
    } else {
        $self->dbh->do($sql) or die $self->dbh->errstr;

        say green("Table applied_migrations successfully created");
        return 1;
    }
}

sub run {
    my ($self, $num) = @_;

    unless ($self->_is_applied_migrations_table_exists() ) {
        die $self->_applied_migrations_not_exist_phrase();
    }

    $self->dbh->{AutoCommit} = 0;

    my $dir  = $self->_detect_dir; 
    my @dirs = sort $self->_dir_listing($dir);

    $num = $num || @dirs;
    my $completed = 0;

    for (@dirs) {
        last unless $num;
        unless ($self->_is_migration_applied($_) ) {
            $self->_run_migration($dir, $_, UP);
            $completed++;
            $num--;
       }
    }

    my $rows = $self->dbh->commit;
    die red("Could't run migrations") if $rows < 0;

    $self->dbh->{AutoCommit} = 1;

    say green("Run migrations:$completed complete");

    return 1;
}

sub rollback {
    my ($self, $num) = @_;

    unless ($self->_is_applied_migrations_table_exists() ) {
        die $self->_applied_migrations_not_exist_phrase();
    }

    $self->dbh->{AutoCommit} = 0;

    my $dir  = $self->_detect_dir; 
    my @dirs = sort { $b cmp $a } $self->_dir_listing($dir);

    $num = $num || @dirs;
    my $completed = 0;

    for (@dirs) {
        last unless $num;
        if ($self->_is_migration_applied($_) ) {
            $self->_run_migration($dir, $_, DOWN);
            $completed++;
            $num--;
       }
    }

    my $rows = $self->dbh->commit;
    die red("Could't rollback migrations") if $rows < 0;

    $self->dbh->{AutoCommit} = 1;

    say green("Rollback migrations:$completed complete");

    return 1;
}

sub _is_applied_migrations_table_exists {
    my ($self) = @_;

    my $sth = $self->dbh->table_info('%', '%', 'applied_migrations', 'TABLE');
    my @row = $sth->fetchrow_array;

    return @row ? 1 : 0;
}

sub _applied_migrations_not_exist_phrase {
    return red("Table applied_migrations does not exists. You should run init first");
}

sub _detect_dir {
    my ($self) = @_;

    return $self->dir               if -d $self->dir;
    return $ENV{PWD}.$self->dir     if -d $ENV{PWD}.$self->dir;
    return $ENV{PWD}.'/'.$self->dir if -d $ENV{PWD}.'/'.$self->dir;

    die red("$self->{dir} doesn't exists");
}

sub _dir_listing {
    my ($self, $dir) = @_;

    opendir my $dh, $dir or die red("Couldn't open dir '$dir': $!");
    my @dirs = readdir $dh;
    closedir $dh;

    return grep !/^\.|\.\.$/, @dirs;
}

sub _is_migration_applied {
    my ($self, $migration) = @_;

    my $sql = 'SELECT migration FROM applied_migrations WHERE migration = ?';
    my $sth = $self->dbh->prepare($sql) or die $self->dbh->errstr;
    my $rv  = $sth->execute($migration);

    die $sth->errstr if $rv < 0;
    
    return $sth->fetchrow_array;
}

sub _run_migration {
    my ($self, $dir, $migration, $type) = @_;

    my $filename = "${migration}_$type.sql";
    my $sql      = read_text "$dir/$migration/$filename";
    my $rows     = $self->dbh->do($sql); 

    die $self->db->errstr if $rows < 0;

    if ($type eq UP) {
        $self->_save_migration($migration);
    }
    else {
        $self->_delete_migration($migration);
    }

    return 1;
}

sub _save_migration {
    my ($self, $migration) = @_;

    my $sql = 'INSERT INTO applied_migrations VALUES(?)';
    my $sth = $self->dbh->prepare($sql) or die $self->dbh->errstr;
    my $rv  = $sth->execute($migration);

    return $rv ne '0E0';
}

sub _delete_migration {
    my ($self, $migration) = @_;

    my $sql = 'DELETE FROM applied_migrations WHERE migration = ?';
    my $sth = $self->dbh->prepare($sql) or die $self->dbh->errstr;
    my $rv  = $sth->execute($migration);

    return $rv ne '0E0';
}

__PACKAGE__

__DATA__
CREATE TABLE applied_migrations (
    migration TEXT
);