import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--state_fips', nargs = '+', type = int, required = True)
    args = parser.parse_args()
    
    print(args.state_fips)