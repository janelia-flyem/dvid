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
build_requirements += recipe_meta['requirements']['host']

print("Installing compiled dependencies...")

# Convert the requirements (with version specs, if any) to conda's command-line syntax
# (i.e. replace spaces with '=')
requirement_specs = map(lambda r: '='.join(r.split()), build_requirements)

# Install to the currently active environment.
cmd = 'conda install -y {}'.format(' '.join(requirement_specs))
print(cmd)
subprocess.check_call(cmd, shell=True)

print("Done installing compiled dependencies.")
