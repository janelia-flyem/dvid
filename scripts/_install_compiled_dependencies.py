import subprocess
import yaml

print("Determining compiled dependencies...")
print("(This may take several seconds.)")

recipe_yaml = '/tmp/dvid-rendered-recipe-meta.yaml'
subprocess.check_call(f'conda render --file {recipe_yaml} conda-recipe', shell=True)

with open(recipe_yaml, 'r') as f:
    recipe_meta = yaml.load(f)

build_requirements = recipe_meta['requirements']['build']

print("Installing compiled dependencies...")
for requirement_spec in map(lambda r: '='.join(r.split()), build_requirements):
    cmd = f'conda install -y {requirement_spec}'
    print(cmd)
    subprocess.check_call(cmd, shell=True)

print("Done installing compiled dependencies.")
