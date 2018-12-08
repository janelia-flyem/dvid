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
recipe_yaml = '/tmp/dvid-rendered-recipe-meta.yaml'
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
    if req.startswith('go'):
        relaxed_build_requirements.append(req)
    else:
        relaxed_build_requirements.append( req.split()[0] )

host_requirements = recipe_meta['requirements']['host']

combined_requirements = relaxed_build_requirements + host_requirements

print("Installing compiled dependencies...")

# Convert the requirements (with version specs, if any)
# to conda's command-line syntax (i.e. replace spaces with '=')
requirement_specs = map(lambda r: '='.join(r.split()), combined_requirements)

# Install to the currently active environment.
cmd = 'conda install -y {}'.format(' '.join(requirement_specs))
print(cmd)
subprocess.check_call(cmd, shell=True)

print("Done installing compiled dependencies.")
