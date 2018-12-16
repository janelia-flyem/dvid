import os
import datetime
import tempfile
import subprocess
import yaml

print("Determining compiled dependencies...")
print("(This may take several seconds.)")

# The compiled dependencies are listed in conda-recipe/meta.yaml,
# in the requirements:build section.
# We can't simply parse meta.yaml without preprocessing it first,
# because technically, meta.yaml is not pure YAML.
# (For instance, it contains jinja template syntax.)
# The 'conda render' command preprocesses the file and converts it to pure YAML,
# which we can then parse with the standard 'yaml' module.
tmpdir = tempfile.mkdtemp()
recipe_yaml = '{}/dvid-rendered-recipe-meta.yaml'.format(tmpdir)
subprocess.check_call('conda render --file {} conda-recipe'.format(recipe_yaml), shell=True)

with open(recipe_yaml, 'r') as f:
    recipe_meta = yaml.load(f)

build_requirements = recipe_meta['requirements']['build']

# For development setup, we want to install everything into one environment.
# That means the 'build' and 'host' environments must all be installed together.
# To avoid letting the 'build' requirements (such as libgcc) conflict with
# the 'host' requirements, we'll relax the version constraints on them,
# except for the go compiler.
relaxed_build_requirements = []
for req in build_requirements:
    if req.startswith('go '):
        relaxed_build_requirements.append('='.join(req.split()[:2]))
    else:
        relaxed_build_requirements.append( req.split()[0] )

host_requirements = recipe_meta['requirements']['host']

combined_requirements = relaxed_build_requirements + host_requirements

print("Installing compiled dependencies...")

# Convert the requirements (with version specs, if any)
# to conda's command-line syntax (i.e. replace spaces with '=')
requirement_specs = map(lambda r: '='.join(r.split()[:2]), combined_requirements)

# Install to the currently active environment.
cmd = 'conda install -y {}'.format(' '.join(requirement_specs))
print(cmd)
subprocess.check_call(cmd, shell=True)

# Remove the conda history.
# -------------------------
#
# Why, you ask?
#
# Conda has some smarts to remember how each package was installed into an environment.
# In particular, it remembers the difference between the following two commands:
#
#   conda install foo
#   conda install foo=1.1
#
# In the first case, you didn't request an exact version, so in the future,
# conda will consider upgrading (or downgrading) foo if that is necessary
# to support any other packages you want to install.
#
# But in the second case, you're explicitly specifiying an exact version of foo.
# Conda thinks you really care about that exact version, so it won't automatically
# update it if some future package requires a different version of foo.
# Instead, it will return an error like this:
#
#   UnsatisfiableError: The following specifications were found to be in conflict:
#   - foo=1.1
#   - bar=2.0
#
# In the above script, we installed EVERYTHING via the second method, meaning it will
# be very annoying for the user to install new packages or upgrade existing ones.
# By removing the history, we remove those constraints on all packages.
#
# Technically, this means we've removed any pre-existing constraints that may have
# existed in the environment previously.  But DVID developers generally don't know
# care about those, and would rather have the flexibility of installing whatever
# they choose.
#
history_path = os.environ["CONDA_PREFIX"] + '/conda-meta/history'
if os.path.exists(history_path):
    timestamp = str(datetime.datetime.now()).replace(' ', '-').replace(':', '-')
    new_path = history_path + '-deleted-' + timestamp
    print("Note: Removing conda history file, backing up as {}".format(new_path))
    os.rename(history_path, new_path)

    # Create empty history file -- required by some conda commands, e.g. 'conda list'
    open(history_path, 'w').close()

print("Done installing compiled dependencies.")
