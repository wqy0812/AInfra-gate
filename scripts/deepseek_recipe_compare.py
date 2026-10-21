#!/usr/bin/env python3
"""File-only DeepSeek reference comparison. Never calls inference or tools."""
import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile

from model_admission import validate_sse

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'testdata/deepseek-recipe/manifest.json'
VERSION = '0.2.0'
MISSING = {'missing': True}


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def load_json(raw):
    def constant(value):
        raise ValueError('non-JSON constant: ' + value)
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key: ' + key)
            result[key] = value
        return result
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError('non-finite JSON number: ' + value)
        return number
    return json.loads(raw, parse_constant=constant, parse_float=finite_float, object_pairs_hook=pairs)


def relative_file(name):
    if not isinstance(name, str):
        raise ValueError('fixture path must be a string')
    path = (ROOT / name).resolve()
    if Path(name).is_absolute() or not path.is_relative_to(ROOT):
        raise ValueError('fixture path must be repository-relative: ' + str(name))
    return path


def load_recipe():
    try:
        version = importlib.metadata.version('deepseek-recipe')
    except importlib.metadata.PackageNotFoundError as error:
        raise ValueError('install scripts/requirements-deepseek-recipe.txt in a virtual environment first') from error
    if version != VERSION:
        raise ValueError(f'deepseek-recipe must be exactly {VERSION}; installed: {version}')
    import deepseek_recipe as recipe
    if recipe.__version__ != VERSION:
        raise ValueError('deepseek_recipe native version does not match the pinned distribution')
    return recipe


def require_fields(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ValueError('unsupported fields in ' + label)


def require_string(value, label):
    if not isinstance(value, str) or not value:
        raise ValueError(label + ' must be a nonempty string')


def validate_pointer(path):
    if not isinstance(path, str) or not path.startswith('/') or re.search(r'~(?![01])', path):
        raise ValueError('check path must be a JSON Pointer beginning with /')


def load_manifest(path):
    manifest = load_json(path.read_bytes())
    require_fields(manifest, ('schema_version', 'policies', 'requests', 'streams', 'wire_cases'), 'manifest')
    if type(manifest['schema_version']) is not int or manifest['schema_version'] != 1:
        raise ValueError('unsupported manifest schema')
    fields = {
        'requests': {'id', 'source', 'request', 'backend', 'target_model', 'checks'},
        'streams': {'id', 'source', 'request', 'policy', 'output', 'finish', 'checks'},
        'wire_cases': {'id', 'source', 'request', 'sse', 'checks'},
    }
    for section in ('policies', 'requests', 'streams', 'wire_cases'):
        if not isinstance(manifest[section], list) or not manifest[section]:
            raise ValueError('manifest section must be a nonempty array: ' + section)
        ids = set()
        for case in manifest[section]:
            if not isinstance(case, dict):
                raise ValueError('manifest entries must be objects')
            ident = case.get('id', '')
            if not isinstance(ident, str) or not re.fullmatch(r'[a-z0-9_-]+', ident) or ident in ids:
                raise ValueError('invalid or duplicate ID in ' + section)
            ids.add(ident)
            if section == 'policies':
                if set(case) != {'id', 'model_version', 'default_thinking_mode', 'reasoning_content', 'model_aliases'}:
                    raise ValueError('every conversion policy must be explicit')
                if case['model_version'] not in ('v4', 'v41') or type(case['default_thinking_mode']) is not bool or case['reasoning_content'] not in ('fill_empty', 'reject'):
                    raise ValueError('invalid conversion policy: ' + ident)
                if not isinstance(case['model_aliases'], list) or any(not isinstance(alias, str) or not alias for alias in case['model_aliases']):
                    raise ValueError('model_aliases must be a list of explicit model names')
            else:
                require_fields(case, fields[section], ident)
                require_fields(case['source'], ('path', 'test', 'note'), ident + '.source')
                for key, value in case['source'].items():
                    require_string(value, ident + '.source.' + key)
                if not isinstance(case['checks'], list) or not case['checks']:
                    raise ValueError('every fixture needs nonempty checks: ' + ident)
                for check in case['checks']:
                    require_fields(check, ('path', 'equals'), ident + '.check')
                    validate_pointer(check['path'])
                request = case['request']
                if not isinstance(request, dict) or set(request) not in ({'body'}, {'file', 'sha256'}):
                    raise ValueError('request must contain body or file + sha256: ' + ident)
                if 'body' in request and not isinstance(request['body'], dict):
                    raise ValueError('request body must be an object: ' + ident)
                if 'file' in request:
                    relative_file(request['file'])
                    if not isinstance(request['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', request['sha256']):
                        raise ValueError('request sha256 must be 64 lowercase hex digits: ' + ident)
                if section == 'requests':
                    require_string(case['target_model'], ident + '.target_model')
                    backend = case['backend']
                    require_fields(backend, ('overrides', 'compatibility'), ident + '.backend')
                    if not isinstance(backend['overrides'], dict):
                        raise ValueError('overrides must be an object: ' + ident)
                    compatibility = backend['compatibility']
                    require_fields(compatibility, ('metadata_values', 'drop_fields'), ident + '.compatibility')
                    if compatibility['metadata_values'] not in ('preserve', 'json_string', 'workspace_object_json_string'):
                        raise ValueError('unsupported metadata policy: ' + ident)
                    drops = compatibility['drop_fields']
                    if not isinstance(drops, list) or any(not isinstance(field, str) or not field for field in drops):
                        raise ValueError('drop_fields must be an array of field names: ' + ident)
                    if any(field in ('model', 'messages', 'stream') for field in drops):
                        raise ValueError('cannot drop protected request fields: ' + ident)
                elif section == 'streams':
                    require_string(case['policy'], ident + '.policy')
                    if not isinstance(case['output'], str) or case['finish'] not in ('Stop', 'Length', 'ContentFilter', None):
                        raise ValueError('invalid raw output or backend finish reason: ' + ident)
                elif not isinstance(case['sse'], str):
                    raise ValueError('sse must be a string: ' + ident)
    policy_ids = {policy['id'] for policy in manifest['policies']}
    if any(case['policy'] not in policy_ids for case in manifest['streams']):
        raise ValueError('stream fixture references an unknown policy')
    return manifest


def request_body(case):
    request = case['request']
    if not isinstance(request, dict):
        raise ValueError('request fixture must be an object')
    if set(request) == {'file', 'sha256'}:
        raw = relative_file(request['file']).read_bytes()
        if sha256(raw) != request['sha256']:
            raise ValueError('fixture hash mismatch: ' + request['file'])
        body = load_json(raw)
        identity = {'file': request['file'], 'sha256': sha256(raw), 'bytes': len(raw)}
    elif set(request) == {'body'}:
        body = request['body']
        identity = {'sha256': sha256(json_bytes(body)), 'encoding': 'canonical-json'}
    else:
        raise ValueError('request must contain body or file + sha256')
    if not isinstance(body, dict):
        raise ValueError('request must be a JSON object')
    return body, identity


def options(recipe, policy):
    version = recipe.ModelVersion.DeepseekV4Pro if policy['model_version'] == 'v4' else recipe.ModelVersion.DeepseekV41Flash
    result = recipe.ConversionOptions(
        default_thinking_mode=policy['default_thinking_mode'],
        reasoning_content_behavior=recipe.ReasoningContentBehavior.Reject if policy['reasoning_content'] == 'reject' else recipe.ReasoningContentBehavior.FillEmpty,
        thinking_max_tokens_upper_bound=393216,
        non_thinking_max_tokens_upper_bound=393216,
    )
    for alias in policy['model_aliases']:
        result = result.with_model_alias(alias, version)
    return result


def effective_policy(recipe, policy):
    resolved = options(recipe, policy)
    tokens = ('Bos', 'Eos', 'ThinkingStart', 'ThinkingEnd', 'System', 'User', 'Assistant',
              'LatestReminder', 'Dsml', 'Image', 'Ref', 'RefEnd', 'Box', 'BoxEnd',
              'Point', 'PointEnd', 'Polygon', 'PolygonEnd')
    return {
        **policy,
        'effective_model_versions_by_name': {name: str(version) for name, version in sorted(resolved.model_versions_by_name.items())},
        'thinking_max_tokens_upper_bound': resolved.thinking_max_tokens_upper_bound,
        'non_thinking_max_tokens_upper_bound': resolved.non_thinking_max_tokens_upper_bound,
        'responses_web_search': str(resolved.responses_web_search),
        'messages_web_search': str(resolved.messages_web_search),
        'special_tokens': {name: resolved.special_tokens.token(getattr(recipe.SpecialToken, name)) for name in tokens},
    }


def text_summary(text):
    if text is None:
        return None
    raw = text.encode()
    return {'characters': len(text), 'utf8_bytes': len(raw), 'sha256': sha256(raw)}


def error_summary(error):
    return {'status': 'error', 'error': {'type': type(error).__name__, 'message': str(error)}}


def conversion(recipe, body, policy):
    try:
        converted = recipe.ChatCompletionRequest(body).convert(options(recipe, policy))
        encoding = recipe.DeepseekV4Encoding() if policy['model_version'] == 'v4' else recipe.DeepseekV41Encoding()
        rendered = encoding.render_conversation(converted.conversation)
    except (ValueError, recipe.ConversionError) as error:
        return error_summary(error)
    conversation = converted.conversation
    messages = []
    for message in conversation.messages:
        messages.append({
            'role': message.role, 'content': text_summary(message.content),
            'reasoning_content': text_summary(message.reasoning_content),
            'tool_calls': [{'id': tool.id, 'name': tool.name, 'arguments': text_summary(tool.arguments)} for tool in message.tool_calls or []],
            'tool_call_id': message.tool_call_id, 'prefix': message.prefix,
            'images': [{'kind': image.kind, 'detail': image.detail, 'path': image.path} for image in message.image_sources],
        })
    inference = converted.inference_options
    parsing = converted.parsing_options
    constraint = inference.output_constraint
    summary = {
        'conversation': {
            'thinking_mode': conversation.thinking_mode, 'reasoning_effort': conversation.reasoning_effort,
            'message_count': len(messages), 'tool_count': len(conversation.tools),
            'empty_assistant_reasoning_count': sum(message['role'] == 'assistant' and message['reasoning_content'] == text_summary('') for message in messages),
            'tool_choice': conversation.tool_choice, 'response_format': conversation.response_format,
            'messages': messages,
            'tools': [{'name': tool.name, 'description': text_summary(tool.description),
                       'parameters_sha256': sha256(json_bytes(tool.parameters)), 'strict': tool.strict,
                       'defer_loading': tool.defer_loading} for tool in conversation.tools],
        },
        'inference': {'max_tokens': inference.max_tokens, 'temperature': inference.temperature, 'top_p': inference.top_p,
                      'seed': inference.seed, 'logprobs': inference.logprobs, 'top_logprobs': inference.top_logprobs,
                      'output_constraint': None if constraint is None else {'kind': constraint.kind, 'schema_sha256': sha256(constraint.schema.encode())}},
        'parsing': {'parse_tool_calls': parsing.parse_tool_calls, 'starts_in_tool_calls': parsing.starts_in_tool_calls,
                    'parse_json_output': parsing.parse_json_output,
                    'thinking_initial_stage': None if parsing.thinking_initial_stage is None else str(parsing.thinking_initial_stage),
                    'stop_sequences': parsing.stop_sequences},
        'prompt': {**text_summary(rendered.prompt), 'special_range_count': len(rendered.special_ranges),
                   'special_ranges_sha256': sha256(json_bytes(rendered.special_ranges))},
    }
    return {'status': 'ok', 'summary': summary}


def differences(before, after, path=''):
    """Typed structural diff with JSON Pointer paths, including removed cases."""
    if isinstance(before, dict) and isinstance(after, dict):
        result = []
        for key in sorted(before.keys() | after.keys()):
            pointer = path + '/' + key.replace('~', '~0').replace('/', '~1')
            if key not in before or key not in after:
                result.append({'path': pointer, 'before': before.get(key, MISSING), 'after': after.get(key, MISSING)})
            else:
                result.extend(differences(before[key], after[key], pointer))
        return result
    if isinstance(before, list) and isinstance(after, list):
        result = []
        for index in range(max(len(before), len(after))):
            if index >= len(before) or index >= len(after):
                result.append({'path': path + '/' + str(index), 'before': before[index] if index < len(before) else MISSING, 'after': after[index] if index < len(after) else MISSING})
            else:
                result.extend(differences(before[index], after[index], path + '/' + str(index)))
        return result
    if type(before) is not type(after) or before != after:
        return [{'path': path or '/', 'before': before, 'after': after}]
    return []


def pointer(value, path):
    validate_pointer(path)
    try:
        for key in path[1:].split('/'):
            key = key.replace('~1', '/').replace('~0', '~')
            if isinstance(value, list):
                if not re.fullmatch(r'0|[1-9][0-9]*', key):
                    return MISSING
                value = value[int(key)]
            else:
                value = value[key]
    except (KeyError, IndexError, ValueError, TypeError):
        return MISSING
    return value


def check_case(observed, checks):
    results = []
    for check in checks:
        if set(check) != {'path', 'equals'}:
            raise ValueError('each check requires path and equals')
        actual = pointer(observed, check['path'])
        found = actual is not MISSING
        passed = found and not differences(check['equals'], actual)
        results.append({**check, 'actual': actual, 'found': found, 'passed': passed})
    return results


def canonical_events(events):
    choices = {}
    ids = []

    def string(value, label):
        if value is None:
            return ''
        if not isinstance(value, str):
            raise ValueError('invalid protocol string: ' + label)
        return value

    def protocol_index(value):
        if type(value) is not int or value < 0:
            raise ValueError('invalid protocol index')
        return value

    for event in events:
        if not isinstance(event, dict) or event.get('error') is not None or not isinstance(event.get('choices'), list):
            raise ValueError('invalid or error protocol event')
        for choice in event['choices']:
            if not isinstance(choice, dict):
                raise ValueError('invalid protocol choice')
            choice_index = protocol_index(choice.get('index'))
            target = choices.setdefault(choice_index, {'index': choice_index, 'content': '', 'reasoning': '', 'refusal': '', 'finish_reasons': [], 'tools': {}})
            delta = choice.get('delta')
            delta = {} if delta is None else delta
            if not isinstance(delta, dict) or delta.get('role') not in (None, 'assistant'):
                raise ValueError('invalid protocol delta or role')
            content = string(delta.get('content'), 'content')
            reasoning = string(delta.get('reasoning_content'), 'reasoning_content')
            refusal = string(delta.get('refusal'), 'refusal')
            tools = delta.get('tool_calls')
            tools = [] if tools is None else tools
            if not isinstance(tools, list):
                raise ValueError('invalid protocol tool_calls')
            finish = string(choice.get('finish_reason'), 'finish_reason')
            if target['finish_reasons'] and (content or reasoning or refusal or tools or finish):
                raise ValueError('protocol output after finish')
            target['content'] += content
            target['reasoning'] += reasoning
            target['refusal'] += refusal
            if finish:
                target['finish_reasons'].append(finish)
            for tool in tools:
                if not isinstance(tool, dict):
                    raise ValueError('invalid protocol tool call')
                tool_index = protocol_index(tool.get('index'))
                aggregate = target['tools'].setdefault(tool_index, {'index': tool_index, 'name': '', 'arguments': '', 'id': '', 'type': ''})
                for field in ('id', 'type'):
                    value = string(tool.get(field), 'tool.' + field)
                    if value:
                        if aggregate[field] and aggregate[field] != value:
                            raise ValueError('conflicting protocol tool ' + field)
                        aggregate[field] = value
                function = tool.get('function', {})
                if not isinstance(function, dict):
                    raise ValueError('invalid protocol tool function')
                aggregate['name'] += string(function.get('name'), 'function.name')
                aggregate['arguments'] += string(function.get('arguments'), 'function.arguments')
    for target in choices.values():
        tools = []
        for index in sorted(target['tools']):
            tool = target['tools'][index]
            ids.append(tool.pop('id'))
            if tool.pop('type') != 'function' or not tool['name']:
                raise ValueError('invalid protocol tool type or name')
            # Arguments are compared as JSON values, independently of whitespace.
            tool['arguments'] = load_json(tool['arguments'])
            tools.append(tool)
        target['tools'] = tools
    if not all(ids) or len(set(ids)) != len(ids):
        raise ValueError('missing or duplicate protocol tool ID')
    return {'choices': [choices[index] for index in sorted(choices)], 'tool_ids_valid': True}


def recipe_stream(recipe, case, policy, splitting):
    body, _ = request_body(case)
    converted = recipe.ChatCompletionRequest(body).convert(options(recipe, policy))
    processor = recipe.StreamProcessor(recipe.ChatCompletionRequest.chunk_generator(converted, 'offline-reference', body['model']), converted.parsing_options)
    events = []
    try:
        # Counts are synthetic and held constant across chunkings, never measured usage.
        events.extend(processor.push(recipe.InferenceChunk.ready(prompt_usage=recipe.PromptUsage(prompt_tokens=0))))
        pieces = list(case['output']) if splitting == 'characters' else [case['output']]
        for piece in pieces:
            events.extend(processor.push(recipe.InferenceChunk.text(piece, token_count=0)))
        if case['finish'] is not None:
            events.extend(processor.push(recipe.InferenceChunk.finish(getattr(recipe.InferenceFinishReason, case['finish']))))
        events.extend(processor.finish())
        raw_events = [event.to_json() for event in events]
    finally:
        processor.close()
    wire = ''.join('data: ' + event + '\n\n' for event in raw_events)
    # HTTP framing is the caller's responsibility. Record our explicit adapter.
    wire += 'data: ' + recipe.ChatCompletionResponse.sse_done_data() + '\n\n'
    return {'status': 'ok', 'canonical': canonical_events([load_json(event) for event in raw_events]),
            'transport': 'caller adds SSE data framing and [DONE]; no HTTP'}, wire


def run_bridge(tasks):
    with tempfile.TemporaryDirectory(prefix='aigate-recipe-') as directory:
        source, destination = Path(directory) / 'input.json', Path(directory) / 'output.json'
        # Preserve nested object order: makePayload keeps unchanged raw JSON,
        # and tool/schema ordering can affect the reference prompt.
        source.write_text(json.dumps(tasks, ensure_ascii=False, separators=(',', ':'), allow_nan=False))
        env = {**os.environ, 'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOTOOLCHAIN': 'local',
               'GOFLAGS': '', 'AIGATE_RECIPE_INPUT': str(source), 'AIGATE_RECIPE_OUTPUT': str(destination)}
        command = ['go', 'test', '-mod=vendor', '-run', '^TestDeepseekRecipeOfflineBridge$', '-count=1', '.']
        process = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
        if process.returncode:
            raise ValueError('offline Go bridge failed:\n' + process.stdout + process.stderr)
        if not destination.exists():
            raise ValueError('Go bridge did not produce evidence')
        return load_json(destination.read_bytes())


def run_suite(recipe, manifest):
    policies = {policy['id']: policy for policy in manifest['policies']}
    tasks, requests, streams = [], {}, {}
    for case in manifest['requests']:
        body, identity = request_body(case)
        backend = case['backend']
        if set(backend) != {'overrides', 'compatibility'} or set(backend['compatibility']) != {'metadata_values', 'drop_fields'}:
            raise ValueError('fixture backend must specify overrides and compatibility explicitly')
        if backend['compatibility']['metadata_values'] not in ('preserve', 'json_string', 'workspace_object_json_string'):
            raise ValueError('unsupported metadata policy')
        if any(field in ('model', 'messages', 'stream') for field in backend['compatibility']['drop_fields']):
            raise ValueError('cannot drop protected request fields')
        requests[case['id']] = (case, body, identity)
        tasks.append({'id': 'request/' + case['id'], 'kind': 'request', 'request': body,
                      'backend': backend, 'model': case['target_model']})
    for case in manifest['streams']:
        if case['finish'] not in ('Stop', 'Length', 'ContentFilter', None):
            raise ValueError('invalid backend finish reason')
        body, identity = request_body(case)
        result = {'source': case['source'], 'input': identity, 'policy': case['policy'],
                  'raw_output': text_summary(case['output']), 'backend_finish': case['finish']}
        for splitting in ('whole', 'characters'):
            try:
                value, wire = recipe_stream(recipe, case, policies[case['policy']], splitting)
            except (ValueError, RuntimeError, recipe.ConversionError) as error:
                result[splitting] = error_summary(error)
                continue
            result[splitting] = value
            tasks.append({'id': f'stream/{case["id"]}/{splitting}', 'kind': 'sse', 'request': body, 'sse': wire})
        result['chunking_differences'] = differences(result['whole'].get('canonical', result['whole']), result['characters'].get('canonical', result['characters']))
        streams[case['id']] = result
    for case in manifest['wire_cases']:
        body, _ = request_body(case)
        tasks.append({'id': 'wire/' + case['id'], 'kind': 'sse', 'request': body, 'sse': case['sse']})
    bridge = run_bridge(tasks)
    snapshot = {'recipe_version': VERSION, 'policies': {name: effective_policy(recipe, policy) for name, policy in policies.items()},
                'policy_comparison_reference': manifest['policies'][0]['id'], 'request_cases': {}, 'stream_cases': {}, 'wire_cases': {}}
    for ident, (case, body, identity) in requests.items():
        aigate = bridge['request/' + ident]
        observed = {'aigate': aigate, 'official': {}}
        for name, policy in policies.items():
            original = conversion(recipe, body, policy)
            adapted = conversion(recipe, aigate['payload'], policy) if aigate['status'] == 'ok' else {'status': 'not_run', 'reason': 'aigate_rejected_request'}
            observed['official'][name] = {'original': original, 'adapted': adapted,
                                          'adaptation_differences': differences(original, adapted)}
        checks = check_case(observed, case['checks'])
        # Full request text stays in the source fixture. Report values use hashes.
        changes = differences(body, aigate['payload']) if aigate['status'] == 'ok' else []
        def compact(value):
            return {'json_sha256': sha256(json_bytes(value)), 'json_bytes': len(json_bytes(value))} if len(json_bytes(value)) > 256 else value
        for change in changes:
            change['before'], change['after'] = compact(change['before']), compact(change['after'])
        snapshot['request_cases'][ident] = {
            'source': case['source'], 'input': identity, 'backend': case['backend'], 'target_model': case['target_model'],
            'aigate': {key: value for key, value in aigate.items() if key != 'payload'},
            'adaptation_changes': changes, 'official': observed['official'],
            'policy_differences': {name: differences(observed['official'][manifest['policies'][0]['id']]['adapted'], value['adapted'])
                                   for name, value in observed['official'].items() if name != manifest['policies'][0]['id']},
            'checks': checks,
        }
    for case in manifest['streams']:
        result = streams[case['id']]
        for splitting in ('whole', 'characters'):
            if result[splitting]['status'] != 'ok':
                result[splitting]['aigate'] = {'completion_class': 'not_run', 'reason': 'reference_parser_error'}
                result[splitting]['observer_differences'] = differences('ok', result[splitting]['status'], '/reference/status')
                continue
            observed = bridge[f'stream/{case["id"]}/{splitting}']
            result[splitting]['aigate'] = observed
            canonical = result[splitting]['canonical']
            expected = {'content': ''.join(choice['content'] for choice in canonical['choices']),
                        'tools': [tool for choice in canonical['choices'] for tool in choice['tools']],
                        'tool_ids_valid': canonical['tool_ids_valid']}
            def arguments(raw):
                try:
                    return load_json(raw)
                except ValueError:
                    return {'invalid_json': text_summary(raw)}
            actual = {'content': observed['content'], 'tool_ids_valid': observed['tool_ids_valid'],
                      'tools': [{**tool, 'arguments': arguments(tool['arguments'])} for tool in observed['tools']]}
            result[splitting]['observer_differences'] = differences(expected, actual)
        result['checks'] = check_case(result, case['checks'])
        result['checks'].append({'path': '/chunking_differences', 'equals': [], 'actual': result['chunking_differences'], 'passed': not result['chunking_differences']})
        for splitting in ('whole', 'characters'):
            diff = result[splitting]['observer_differences']
            result['checks'].append({'path': '/' + splitting + '/observer_differences', 'equals': [], 'actual': diff, 'passed': not diff})
        snapshot['stream_cases'][case['id']] = result
    for case in manifest['wire_cases']:
        body, identity = request_body(case)
        try:
            admission = {'status': 'ok', 'summary': validate_sse(case['sse'].encode(), body)}
        except ValueError as error:
            admission = error_summary(error)
        result = {'source': case['source'], 'request': identity, 'wire': text_summary(case['sse']),
                  'official': {'status': 'not_applicable', 'reason': 'already_protocol_SSE_not_raw_model_output'},
                  'aigate': bridge['wire/' + case['id']], 'admission': admission}
        result['checks'] = check_case(result, case['checks'])
        snapshot['wire_cases'][case['id']] = result
    return snapshot


def provenance(recipe, manifest_path, manifest):
    files = ['adapter.go', 'sse_observer.go', 'deepseek_recipe_offline_test.go',
             'scripts/model_admission.py', 'scripts/deepseek_recipe_compare.py', 'scripts/requirements-deepseek-recipe.txt']
    files += [case['request']['file'] for case in manifest['requests'] if 'file' in case['request']]
    files += [case['source']['path'] for section in ('requests', 'streams', 'wire_cases') for case in manifest[section]
              if not case['source']['path'].startswith('https://')]
    native = sys.modules['deepseek_recipe._native']
    return {
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'python': platform.python_version(), 'platform': platform.platform(),
        'go': subprocess.check_output(['go', 'version'], text=True).strip(),
        'recipe_distribution': importlib.metadata.version('deepseek-recipe'), 'recipe_native_sha256': sha256(Path(native.__file__).read_bytes()),
        'manifest_sha256': sha256(manifest_path.read_bytes()),
        'source_sha256': {name: sha256(relative_file(name).read_bytes()) for name in sorted(set(files))},
        'scope': 'local conversion, prompt rendering, synthetic raw text parsing, and file-only Go adapter/observer; no inference, tool execution, HTTP, image fetching, or tokenizer downloads',
        'usage': 'synthetic stream token counts are zero, not real usage; no token-ID encoding or context-capacity validation',
        'aliases': 'evaluation mappings only; no deployment identity is inferred',
    }


def validate_baseline(baseline):
    if not isinstance(baseline, dict) or type(baseline.get('schema_version')) is not int or baseline['schema_version'] != 1:
        raise ValueError('unsupported baseline report')
    snapshot = baseline.get('snapshot')
    if not isinstance(snapshot, dict) or any(not isinstance(snapshot.get(section), dict) for section in ('policies', 'request_cases', 'stream_cases', 'wire_cases')):
        raise ValueError('baseline snapshot must contain policy and case objects')
    if not isinstance(baseline.get('provenance'), dict):
        raise ValueError('baseline provenance must be an object')
    return baseline


def make_report(snapshot, identity, baseline=None):
    checks = [check for section in ('request_cases', 'stream_cases', 'wire_cases') for result in snapshot[section].values() for check in result['checks']]
    comparison = None
    if baseline is not None:
        validate_baseline(baseline)
        comparison = {'differences': differences(baseline['snapshot'], snapshot),
                      'provenance_differences': differences(baseline.get('provenance', {}), identity)}
    failures = sum(not check['passed'] for check in checks)
    return {'schema_version': 1, 'provenance': identity, 'snapshot': snapshot,
            'baseline': comparison,
            'summary': {'checks': len(checks), 'failed_checks': failures,
                        'baseline_differences': 0 if comparison is None else len(comparison['differences']),
                        'passed': failures == 0 and (comparison is None or not comparison['differences'])}}


def render_report(report):
    summary = report['summary']
    lines = ['# DeepSeek 离线对照报告', '',
             f"结果：{'通过' if summary['passed'] else '存在未预期差异'}；检查 {summary['checks']} 项，失败 {summary['failed_checks']} 项，基线差异 {summary['baseline_differences']} 项。", '',
             '预期拒绝和策略差异由夹具显式断言；转换成功不代表真实模型接入成功。模型别名为评估映射。', '',
             '## 请求', '', '| 案例 | AIGate | 请求字段变化 | 官方转换：原始 / 适配后 |', '| --- | --- | ---: | --- |']
    for ident, case in report['snapshot']['request_cases'].items():
        conversions = '; '.join(f"{name}: {value['original']['status']}/{value['adapted']['status']}" for name, value in case['official'].items())
        lines.append(f"| {ident} | {case['aigate']['status']} | {len(case['adaptation_changes'])} | {conversions} |")
    lines += ['', '## 原始模型文本解析', '', '| 案例 | 整段 / 逐字符语义一致 | AIGate 观察分类 |', '| --- | --- | --- |']
    for ident, case in report['snapshot']['stream_cases'].items():
        lines.append(f"| {ident} | {not case['chunking_differences']} | {case['whole']['aigate']['completion_class'] or 'complete'} |")
    lines += ['', '## 已有 SSE 问题夹具', '', '| 案例 | AIGate 观察分类 | 真实接入门禁的离线解析 |', '| --- | --- | --- |']
    for ident, case in report['snapshot']['wire_cases'].items():
        lines.append(f"| {ident} | {case['aigate']['completion_class'] or 'complete'} | {case['admission']['status']} |")
    lines += ['', '字段级差异、输入哈希、来源、全部策略和失败断言见同目录 report.json。',
              '其中 adaptation_changes 是当前 Go 适配器的实际字段变化；official.*.adaptation_differences 是该变化对官方语义的影响；policy_differences 是显式转换策略之间的差异。',
              '基线只比较 snapshot；Python/Go/原生库和源码身份变化另列 provenance_differences。', '']
    if report['baseline']:
        lines += ['## 基线差异路径', '']
        lines += ['- `' + diff['path'] + '`' for diff in report['baseline']['differences']]
        lines.append('')
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, default=MANIFEST)
    parser.add_argument('--output', type=Path, required=True, help='new output directory; never overwritten')
    parser.add_argument('--baseline', type=Path, help='previous report.json; differences cause exit 1')
    args = parser.parse_args(argv)
    try:
        if args.output.exists():
            raise ValueError('output directory already exists: ' + str(args.output))
        recipe = load_recipe()
        manifest = load_manifest(args.manifest)
        baseline = validate_baseline(load_json(args.baseline.read_bytes())) if args.baseline else None
        snapshot = run_suite(recipe, manifest)
        report = make_report(snapshot, provenance(recipe, args.manifest, manifest), baseline)
        args.output.mkdir(parents=True, exist_ok=False)
        (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + '\n')
        (args.output / 'report.md').write_text(render_report(report))
        print(json.dumps({'report': str(args.output / 'report.json'), **report['summary']}, ensure_ascii=False))
        return 0 if report['summary']['passed'] else 1
    except (OSError, ImportError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print('offline comparison setup failed: ' + str(error), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
