Summary:	btier is a tiered block device
Name:		btier
Version:	1.1.0
Release:	1
License:	GPLv3+
Group:		Applications/System
URL:            http://www.lessfs.com
Source:         http://downloads.sourceforge.net/tier/%{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}
BuildRequires:  autoconf

%description
btier is a tiered block device

%prep
%setup -q

%build
export CFLAGS="-O2"
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} install
install -D -m 755 rc/btier %{buildroot}/etc/init.d/btier
install -D -m 755 rc/bttab %{buildroot}/etc/bttab_example

%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-, root, root, -)
%doc FAQ ChangeLog COPYING README
/sbin/btier_setup
/sbin/btier_inspect
/etc/init.d/btier
/etc/bttab_example
/lib/modules/3.0.58-0.6.6-default/kernel/drivers/block/btier.ko
/usr/share/man/man1/btier_inspect.1.gz
/usr/share/man/man1/btier_setup.1.gz

%changelog
* Mon Jul 29 2013 Mark Ruijter <mruijter@gmail.com>  - 1.1.0
- Initial package
