name:      MogileFS-Server
summary:   MoilgeFS-Server - MogileFS Server daemons and utilities.
version:   2.17
release:   1
vendor:    Brad Fitzpatrick <brad@danga.com>
packager:  Jonathan Steinert <hachi@cpan.org>
license:   Artistic
group:     Applications/CPAN
buildroot: %{_tmppath}/%{name}-%{version}-%(id -u -n)
buildarch: noarch
source:    mogilefs-server-%{version}.tar.gz
autoreq:   no
requires:  MogileFS-Server-mogilefsd, MogileFS-Server-mogstored

# Build requires for mogilefsd
buildrequires: perl(DBI), perl(DBD::mysql)
# Build requires for mogstored
buildrequires: perl(Gearman::Client::Async) >= 0.93, perl(Gearman::Server) >= 1.08, perl(Perlbal) >= 1.53

%description
MogileFS Server daemons and utilities.
This is a dummy package which depends on all the others so you can install them all easily.

%prep
rm -rf "%{buildroot}"
%setup -n mogilefs-server-%{version}

%build
%{__perl} Makefile.PL PREFIX=%{buildroot}%{_prefix}
make all
#make test

%install
make pure_install

[ -x /usr/lib/rpm/brp-compress ] && /usr/lib/rpm/brp-compress


# remove special files
find %{buildroot} \(                    \
       -name "perllocal.pod"            \
    -o -name ".packlist"                \
    -o -name "*.bs"                     \
    \) -exec rm -f {} \;

# no empty directories
find %{buildroot}%{_prefix}             \
    -type d -depth -empty               \
    -exec rmdir {} \;

%clean
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}

%files
%defattr(-,root,root)

%package -n MogileFS-Server-mogilefsd
summary:   MogileFS-Server-mogilefsd - Mogilefsd and related libraries.
group:     Applications/CPAN
autoreq:   no
requires:  perl(DBI) >= 1.44, perl(DBD::mysql) >= 3
obsoletes: MogileFS-Server-utils <= 2.16

%description -n MogileFS-Server-mogilefsd
Mogilefsd and related libraries.

%files -n MogileFS-Server-mogilefsd
%defattr(-,root,root)
%{_prefix}/bin/mogilefsd
%{_prefix}/bin/mogdbsetup
%{_prefix}/lib/perl5/site_perl/5.8.5/MogileFS/*
%{_prefix}/share/man/man1/mogilefsd.1.gz
%{_prefix}/share/man/man3/MogileFS::*.3pm.gz

%package -n MogileFS-Server-mogstored
summary:   MogileFS-Server-mogstored - Mogstored and related libraries.
group:     Applications/CPAN
autoreq:   no
requires:  perl-Gearman-Client-Async >= 0.93, perl-Gearman-Server >= 1.08, perl-Perlbal >= 1.53
obsoletes: MogileFS-Server-utils <= 2.16

%description -n MogileFS-Server-mogstored
Mogstored and related libraries.

%files -n MogileFS-Server-mogstored
%defattr(-,root,root)
%{_prefix}/bin/mogstored
%{_prefix}/bin/mogautomount
%{_prefix}/lib/perl5/site_perl/5.8.5/Mogstored/*
%{_prefix}/share/man/man1/mogstored.1.gz
%{_prefix}/share/man/man1/mogautomount.1.gz
