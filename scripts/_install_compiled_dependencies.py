import subprocess
import yaml

print("Determining compiled dependencies...")
print("(This may take several seconds.)")

recipe_yaml = '/tmp/dvid-rendered-recipe-meta.yaml'
subprocess.check_call('conda render --file {} conda-recipe'.format(recipe_yaml), shell=True)

with open(recipe_yaml, 'r') as f:
    recipe_meta = yaml.load(f)

build_requirements = recipe_meta['requirements']['build']

print("Installing compiled dependencies...")
requirement_specs = map(lambda r: '='.join(r.split()), build_requirements)

cmd = 'conda install -y {}'.format(' '.join(requirement_specs))
print(cmd)
subprocess.check_call(cmd, shell=True)

print("Done installing compiled dependencies.")
