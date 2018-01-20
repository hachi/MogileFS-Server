# It's just a little bit of perl, so hang tight if you're a php/etc user :)
use warnings;
use strict;

# Import the MogileFS client and a helper util from Plack.
use Plack::Request;
use MogileFS::Client;

my $TRACKERS = ['tracker1:7001'];
my $DOMAIN   = 'toast';

# Initialize the client when the server starts.
# You could also do this in the middle of the request.
my $mogc = MogileFS::Client->new(domain => $DOMAIN,
    hosts => $TRACKERS);

sub run {
    # Request object for reading paths/cookies/etc.
    my $req = shift;

    # Only support GET requests for this example.
    # Nothing stops us from supporting HEAD requests, though.
    if ($req->method ne 'GET') {
        return [ 403, [ 'Content-Type' => 'text/plain' ],
            [ 'Only GET methods allowed' ] ];
    }

    # Pull out the GET /whatever path.
    my $file = $req->path_info;

    # At this stage you would do some validation, or query your own
    # application database for what the MogileFS path actually is. In this
    # example we just ensure there is a limited set of characters used.
    unless ($file =~ m/^[A-Z0-9.\/\\]+$/gmi) {
        return [ 404, [ 'Content-Type' => 'text/plain' ],
            [ 'Invalid request format received!' ] ];
    }

    # Ask the MogileFS tracker for the paths to this file.
    # At this point you could check memcached for cached paths as well, and
    # cache if none were found.
    my @paths = $mogc->get_paths($file);

    # If MogileFS returns no paths, the file is likely missing or never
    # existed.
    unless (@paths) {
        return [ 404, [ 'Content-Type' => 'text/plain' ],
            [ 'File not found: ' . $file ] ];
    }

    # Now we create the magic Perlbal header, "X-REPROXY-URL". This header
    # tells Perlbal to go fetch and return the file from where MogileFS has
    # said it is.
    # At this point you would add any other headers. If it's a jpeg, you would
    # ship the proper 'image/jpeg' Content-Type. In this example we blanket
    # serve everything as text/plain.
    my $headers = [ 'Content-Type' => 'text/plain',
        'X-REPROXY-URL' => join(' ', @paths) ];

    # Return a 200 OK, the headers, and no body. The body will be filled in
    # with what Perlbal fetches.
    return [ 200, $headers, [ ] ];
}

# Some simple Plack glue, you can ignore this.
# For a real app you should use a full framework. ;)
my $app = sub {
    my $env = shift;
    my $req = Plack::Request->new($env);
    return run($req);
};
